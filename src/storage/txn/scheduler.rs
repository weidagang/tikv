// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! Scheduler which schedules the execution of `storage::Command`s.
//!
//! Scheduler is in between the clients and the underlying storage engine. It receives commands
//! from the clients, executes them with the underlying storage engine. It also keeps track of all
//! the running commands and uses latches to ensure serialized access to the overlapping rows
//! involved in concurrent commands.
//!
//! There is one scheduler for each store, running as a single-thread event loop, but command
//! executions are delegated to a pool of worker threads, the results are then posted back to the
//! event loop.
//!
//! Note that scheduler ensures serialized access to the overlapping rows at command level, but a
//! transaction may consist of multiple commands, therefore conflicts may happen at transaction
//! level. Transaction semantics is ensured by the transaction protocol implemented in the client
//! library, which is transparent to the scheduler.

use std::time::Duration;
use std::boxed::Box;
use std::fmt::{self, Formatter, Debug};
use threadpool::ThreadPool;
use storage::{Engine, Command, Snapshot, StorageCb, Result as StorageResult, Error as StorageError};
use kvproto::kvrpcpb::{Context, LockInfo};
use storage::mvcc::{MvccTxn, MvccReader, Error as MvccError};
use storage::{Key, Value, KvPair};
use std::collections::HashMap;
use mio::{self, EventLoop};
use util::transport::SendCh;
use storage::engine::{Result as EngineResult, Callback as EngineCallback, Modify};
use super::Result;
use super::Error;
use super::store::SnapshotStore;
use super::latch::{Latches, Lock};

const REPORT_STATISTIC_INTERVAL: u64 = 60000; // 60 seconds

// TODO: make it configurable.
const GC_BATCH_SIZE: usize = 512;

pub enum Tick {
    ReportStatistic,
}

/// Process result of a command.
pub enum ProcessResult {
    Res,
    MultiRes { results: Vec<StorageResult<()>> },
    MultiKvpairs { pairs: Vec<StorageResult<KvPair>> },
    Value { value: Option<Value> },
    Locks { locks: Vec<LockInfo> },
    NextCommand { cmd: Command },
    Failed { err: StorageError },
}

/// Message types for the scheduler event loop.
pub enum Msg {
    Quit,
    RawCmd { cmd: Command, cb: StorageCb },
    SnapshotFinished {
        cid: u64,
        snapshot: EngineResult<Box<Snapshot>>,
    },
    ReadFinished { cid: u64, pr: ProcessResult },
    WritePrepareFinished {
        cid: u64,
        cmd: Command,
        pr: ProcessResult,
        to_be_write: Vec<Modify>,
    },
    WritePrepareFailed { cid: u64, err: Error },
    WriteFinished {
        cid: u64,
        pr: ProcessResult,
        result: EngineResult<()>,
    },
}

/// Debug for messages.
impl Debug for Msg {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(f, "Quit"),
            Msg::RawCmd { ref cmd, .. } => write!(f, "RawCmd {:?}", cmd),
            Msg::SnapshotFinished { cid, .. } => write!(f, "SnapshotFinished [cid={}]", cid),
            Msg::ReadFinished { cid, .. } => write!(f, "ReadFinished [cid={}]", cid),
            Msg::WritePrepareFinished { cid, ref cmd, .. } => {
                write!(f, "WritePrepareFinished [cid={}, cmd={:?}]", cid, cmd)
            }
            Msg::WritePrepareFailed { cid, ref err } => {
                write!(f, "WritePrepareFailed [cid={}, err={:?}]", cid, err)
            }
            Msg::WriteFinished { cid, .. } => write!(f, "WriteFinished [cid={}]", cid),
        }
    }
}

/// Delivers the process result of a command to the storage callback.
fn execute_callback(callback: StorageCb, pr: ProcessResult) {
    match callback {
        StorageCb::Boolean(cb) => {
            match pr {
                ProcessResult::Res => cb(Ok(())),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            }
        }
        StorageCb::Booleans(cb) => {
            match pr {
                ProcessResult::MultiRes { results } => cb(Ok(results)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            }
        }
        StorageCb::SingleValue(cb) => {
            match pr {
                ProcessResult::Value { value } => cb(Ok(value)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            }
        }
        StorageCb::KvPairs(cb) => {
            match pr {
                ProcessResult::MultiKvpairs { pairs } => cb(Ok(pairs)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            }
        }
        StorageCb::Locks(cb) => {
            match pr {
                ProcessResult::Locks { locks } => cb(Ok(locks)),
                ProcessResult::Failed { err } => cb(Err(err)),
                _ => panic!("process result mismatch"),
            }
        }
    }
}

/// Context for a running command.
pub struct RunningCtx {
    cid: u64,
    cmd: Option<Command>,
    lock: Lock,
    callback: Option<StorageCb>,
}

impl RunningCtx {
    /// Creates a context for a running command.
    pub fn new(cid: u64, cmd: Command, lock: Lock, cb: StorageCb) -> RunningCtx {
        RunningCtx {
            cid: cid,
            cmd: Some(cmd),
            lock: lock,
            callback: Some(cb),
        }
    }
}

/// Creates a callback to receive async results of write prepare from the storage engine.
fn make_engine_cb(cid: u64, pr: ProcessResult, ch: SendCh<Msg>) -> EngineCallback<()> {
    Box::new(move |result: EngineResult<()>| {
        if let Err(e) = ch.send(Msg::WriteFinished {
            cid: cid,
            pr: pr,
            result: result,
        }) {
            error!("send write finished to scheduler failed cid={}, err:{:?}",
                   cid,
                   e);
        }
    })
}

/// Scheduler which schedules the execution of `storage::Command`s.
pub struct Scheduler {
    engine: Box<Engine>,

    // cid -> context
    cmd_ctxs: HashMap<u64, RunningCtx>,

    schedch: SendCh<Msg>,

    // cmd id generator
    id_alloc: u64,

    // write concurrency control
    latches: Latches,

    // worker pool
    worker_pool: ThreadPool,
}

impl Scheduler {
    /// Creates a scheduler.
    pub fn new(engine: Box<Engine>,
               schedch: SendCh<Msg>,
               concurrency: usize,
               worker_pool_size: usize)
               -> Scheduler {
        Scheduler {
            engine: engine,
            cmd_ctxs: HashMap::new(),
            schedch: schedch,
            id_alloc: 0,
            latches: Latches::new(concurrency),
            worker_pool: ThreadPool::new_with_name(thd_name!("sched-worker-pool"),
                                                   worker_pool_size),
        }
    }
}

/// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
/// event loop.
fn process_read(cid: u64, mut cmd: Command, ch: SendCh<Msg>, snapshot: Box<Snapshot>) {
    debug!("process read cmd(cid={}) in worker pool.", cid);
    let pr = match cmd {
        // Gets from the snapshot.
        Command::Get { ref key, start_ts, .. } => {
            let snap_store = SnapshotStore::new(snapshot.as_ref(), start_ts);
            let res = snap_store.get(key);
            match res {
                Ok(val) => ProcessResult::Value { value: val },
                Err(e) => ProcessResult::Failed { err: StorageError::from(e) },
            }
        }
        // Batch gets from the snapshot.
        Command::BatchGet { ref keys, start_ts, .. } => {
            let snap_store = SnapshotStore::new(snapshot.as_ref(), start_ts);
            match snap_store.batch_get(keys) {
                Ok(results) => {
                    let mut res = vec![];
                    for (k, v) in keys.into_iter().zip(results) {
                        match v {
                            Ok(Some(x)) => res.push(Ok((k.raw().unwrap(), x))),
                            Ok(None) => {}
                            Err(e) => res.push(Err(StorageError::from(e))),
                        }
                    }
                    ProcessResult::MultiKvpairs { pairs: res }
                }
                Err(e) => ProcessResult::Failed { err: StorageError::from(e) },
            }
        }
        // Scans a range starting with `start_key` up to `limit` rows from the snapshot.
        Command::Scan { ref start_key, limit, start_ts, .. } => {
            let snap_store = SnapshotStore::new(snapshot.as_ref(), start_ts);
            let res = snap_store.scanner()
                .and_then(|mut scanner| scanner.scan(start_key.clone(), limit))
                .and_then(|mut results| {
                    Ok(results.drain(..).map(|x| x.map_err(StorageError::from)).collect())
                });
            match res {
                Ok(pairs) => ProcessResult::MultiKvpairs { pairs: pairs },
                Err(e) => ProcessResult::Failed { err: e.into() },
            }
        }
        // Scans locks with timestamp <= `max_ts`
        Command::ScanLock { max_ts, .. } => {
            let mut reader = MvccReader::new(snapshot.as_ref());
            let res = reader.scan_lock(|lock| lock.ts <= max_ts)
                .map_err(Error::from)
                .and_then(|v| {
                    let mut locks = vec![];
                    for (key, lock) in v {
                        let mut lock_info = LockInfo::new();
                        lock_info.set_primary_lock(lock.primary);
                        lock_info.set_lock_version(lock.ts);
                        lock_info.set_key(try!(key.raw()));
                        locks.push(lock_info);
                    }
                    Ok(locks)
                });
            match res {
                Ok(locks) => ProcessResult::Locks { locks: locks },
                Err(e) => ProcessResult::Failed { err: e.into() },
            }
        }
        // Gets the lock with timestamp `start_ts`, then sends either a `Commit` command if the
        // lock has commit timestamp populated or a `Rollback` command otherwise.
        Command::ResolveLock { ref ctx, start_ts, commit_ts } => {
            let mut reader = MvccReader::new(snapshot.as_ref());
            let res = reader.scan_lock(|lock| lock.ts == start_ts)
                .map_err(Error::from)
                .and_then(|v| {
                    let keys = v.into_iter().map(|x| x.0).collect();
                    let next_cmd = match commit_ts {
                        Some(ts) => {
                            Command::Commit {
                                ctx: ctx.clone(),
                                keys: keys,
                                lock_ts: start_ts,
                                commit_ts: ts,
                            }
                        }
                        None => {
                            Command::Rollback {
                                ctx: ctx.clone(),
                                keys: keys,
                                start_ts: start_ts,
                            }
                        }
                    };
                    Ok(next_cmd)
                });
            match res {
                Ok(cmd) => ProcessResult::NextCommand { cmd: cmd },
                Err(e) => ProcessResult::Failed { err: e.into() },
            }
        }
        // Collects garbage.
        Command::Gc { ref ctx, safe_point, ref mut scan_key, .. } => {
            let mut reader = MvccReader::new(snapshot.as_ref());
            let res = reader.scan_keys(scan_key.take(), GC_BATCH_SIZE)
                .map_err(Error::from)
                .and_then(|(keys, next_start)| {
                    Ok(Command::Gc {
                        ctx: ctx.clone(),
                        safe_point: safe_point,
                        scan_key: next_start,
                        keys: keys,
                    })
                });
            match res {
                Ok(cmd) => ProcessResult::NextCommand { cmd: cmd },
                Err(e) => ProcessResult::Failed { err: e.into() },
            }
        }
        _ => panic!("unsupported read command"),
    };

    if let Err(e) = ch.send(Msg::ReadFinished { cid: cid, pr: pr }) {
        // Todo: if this happens we need to clean up command's context
        error!("send read finished failed, cid={}, err={:?}", cid, e);
    }
}

/// Processes a write command within a worker thread, then posts either a `WritePrepareFinished`
/// message if successful or a `WritePrepareFailed` message back to the event loop.
fn process_write(cid: u64, cmd: Command, ch: SendCh<Msg>, snapshot: Box<Snapshot>) {
    if let Err(e) = process_write_impl(cid, cmd, ch.clone(), snapshot.as_ref()) {
        if let Err(err) = ch.send(Msg::WritePrepareFailed { cid: cid, err: e }) {
            // Todo: if this happens, lock will hold for ever
            panic!("send WritePrepareFailed message to channel failed. cid={}, err={:?}",
                   cid,
                   err);
        }
    }
}

fn process_write_impl(cid: u64,
                      mut cmd: Command,
                      ch: SendCh<Msg>,
                      snapshot: &Snapshot)
                      -> Result<()> {
    let (pr, modifies) = match cmd {
        Command::Prewrite { ref mutations, ref primary, start_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, start_ts);
            let mut results = vec![];
            for m in mutations {
                match txn.prewrite(m.clone(), primary) {
                    Ok(_) => results.push(Ok(())),
                    e @ Err(MvccError::KeyIsLocked { .. }) => results.push(e.map_err(Error::from)),
                    Err(e) => return Err(Error::from(e)),
                }
            }
            let res = results.drain(..).map(|x| x.map_err(StorageError::from)).collect();
            let pr = ProcessResult::MultiRes { results: res };
            (pr, txn.modifies())
        }
        Command::Commit { ref keys, lock_ts, commit_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, lock_ts);
            for k in keys {
                try!(txn.commit(&k, commit_ts));
            }

            let pr = ProcessResult::Res;
            (pr, txn.modifies())
        }
        Command::Cleanup { ref key, start_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, start_ts);
            try!(txn.rollback(&key));

            let pr = ProcessResult::Res;
            (pr, txn.modifies())
        }
        Command::Rollback { ref keys, start_ts, .. } => {
            let mut txn = MvccTxn::new(snapshot, start_ts);
            for k in keys {
                try!(txn.rollback(&k));
            }

            let pr = ProcessResult::Res;
            (pr, txn.modifies())
        }
        Command::Gc { ref ctx, safe_point, ref mut scan_key, ref keys } => {
            let mut txn = MvccTxn::new(snapshot, 0);
            for k in keys {
                try!(txn.gc(k, safe_point));
            }
            if scan_key.is_none() {
                (ProcessResult::Res, txn.modifies())
            } else {
                let pr = ProcessResult::NextCommand {
                    cmd: Command::Gc {
                        ctx: ctx.clone(),
                        safe_point: safe_point,
                        scan_key: scan_key.take(),
                        keys: vec![],
                    },
                };
                (pr, txn.modifies())
            }
        }
        _ => panic!("unsupported write command"),
    };

    box_try!(ch.send(Msg::WritePrepareFinished {
        cid: cid,
        cmd: cmd,
        pr: pr,
        to_be_write: modifies,
    }));

    Ok(())
}

/// Extracts the context of a command.
fn extract_ctx(cmd: &Command) -> &Context {
    match *cmd {
        Command::Get { ref ctx, .. } |
        Command::BatchGet { ref ctx, .. } |
        Command::Scan { ref ctx, .. } |
        Command::Prewrite { ref ctx, .. } |
        Command::Commit { ref ctx, .. } |
        Command::CommitThenGet { ref ctx, .. } |
        Command::Cleanup { ref ctx, .. } |
        Command::Rollback { ref ctx, .. } |
        Command::RollbackThenGet { ref ctx, .. } |
        Command::ScanLock { ref ctx, .. } |
        Command::ResolveLock { ref ctx, .. } |
        Command::Gc { ref ctx, .. } => ctx,
    }
}

impl Scheduler {
    /// Generates the next command ID.
    fn gen_id(&mut self) -> u64 {
        self.id_alloc += 1;
        self.id_alloc
    }

    /// Generates the lock for a command.
    fn gen_lock(&self, cmd: &Command) -> Lock {
        match *cmd {
            Command::Prewrite { ref mutations, .. } => {
                let keys: Vec<&Key> = mutations.iter().map(|x| x.key()).collect();
                self.latches.gen_lock(&keys)
            }
            Command::Commit { ref keys, .. } |
            Command::Rollback { ref keys, .. } => self.latches.gen_lock(keys),
            Command::CommitThenGet { ref key, .. } |
            Command::Cleanup { ref key, .. } |
            Command::RollbackThenGet { ref key, .. } => self.latches.gen_lock(&[key]),
            _ => Lock::new(vec![]),
        }
    }

    /// Delivers a command to a worker thread for processing.
    fn process_by_worker(&mut self, cid: u64, snapshot: Box<Snapshot>) {
        debug!("process cmd with snapshot, cid={}", cid);
        let cmd = {
            let ctx = &mut self.cmd_ctxs.get_mut(&cid).unwrap();
            assert_eq!(ctx.cid, cid);
            ctx.cmd.take().unwrap()
        };
        let ch = self.schedch.clone();
        let readcmd = cmd.readonly();
        if readcmd {
            self.worker_pool.execute(move || process_read(cid, cmd, ch, snapshot));
        } else {
            self.worker_pool.execute(move || process_write(cid, cmd, ch, snapshot));
        }
    }

    /// Calls the callback with an error.
    fn finish_with_err(&mut self, cid: u64, err: Error) {
        debug!("command cid={}, finished with error", cid);

        let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        let cb = ctx.callback.take().unwrap();
        let pr = ProcessResult::Failed { err: StorageError::from(err) };
        execute_callback(cb, pr);

        self.release_lock(&ctx.lock, cid);
    }

    /// Extracts the context of a command.
    fn extract_context(&self, cid: u64) -> &Context {
        let ctx = &self.cmd_ctxs.get(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        extract_ctx(ctx.cmd.as_ref().unwrap())
    }

    fn register_report_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::ReportStatistic,
                                       REPORT_STATISTIC_INTERVAL) {
            error!("register report statistic err: {:?}", e);
        };
    }

    fn on_report_staticstic_tick(&self, event_loop: &mut EventLoop<Self>) {
        info!("all running cmd count = {}", self.cmd_ctxs.len());

        self.register_report_tick(event_loop);
    }

    /// Event handler for new command.
    ///
    /// This method will try to acquire all the necessary latches. If successful, initiates a get
    /// snapshot for furthur processing; otherwise, adds the command to the waiting queue(s), it
    /// will be handled later in `wakeup_cmd` when its turn comes.
    ///
    /// Note that once a command is ready to execute, the snapshot is always up-to-date during the
    /// execution because 1) all the conflicting commands (if any) must be in the waiting queues;
    /// 2) there may be non-conflicitng commands running concurrently, but it doesn't matter.
    fn on_receive_new_cmd(&mut self, cmd: Command, callback: StorageCb) {
        let cid = self.gen_id();
        debug!("received new command, cid={}, cmd={}", cid, cmd);
        let lock = self.gen_lock(&cmd);
        let ctx = RunningCtx::new(cid, cmd, lock, callback);
        if self.cmd_ctxs.insert(cid, ctx).is_some() {
            panic!("command cid={} shouldn't exist", cid);
        }

        if self.acquire_lock(cid) {
            self.get_snapshot(cid);
        }
    }

    /// Tries to acquire all the required latches for a command.
    ///
    /// Returns true if successful; returns false otherwise.
    fn acquire_lock(&mut self, cid: u64) -> bool {
        let ctx = &mut self.cmd_ctxs.get_mut(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        self.latches.acquire(&mut ctx.lock, cid)
    }

    /// Initiates an async operation to get a snapshot from the storage engine, then posts a
    /// `SnapshotFinished` message back to the event loop when it finishes.
    fn get_snapshot(&mut self, cid: u64) {
        let ch = self.schedch.clone();
        let cb = box move |snapshot: EngineResult<Box<Snapshot>>| {
            if let Err(e) = ch.send(Msg::SnapshotFinished {
                cid: cid,
                snapshot: snapshot,
            }) {
                error!("send SnapshotFinish failed cmd id {}, err {:?}", cid, e);
            }
        };

        if let Err(e) = self.engine.async_snapshot(self.extract_context(cid), cb) {
            self.finish_with_err(cid, Error::from(e));
        }
    }

    /// Event handler for the completion of get snapshot.
    ///
    /// Delivers the command along with the snapshot to a worker thread to execute.
    fn on_snapshot_finished(&mut self, cid: u64, snapshot: EngineResult<Box<Snapshot>>) {
        debug!("receive snapshot finish msg for cid={}", cid);
        match snapshot {
            Ok(snapshot) => self.process_by_worker(cid, snapshot),
            Err(e) => self.finish_with_err(cid, Error::from(e)),
        }
    }

    /// Event handler for the success of read.
    ///
    /// If a next command is present, continues to execute; otherwise, delivers the result to the
    /// callback.
    fn on_read_finished(&mut self, cid: u64, pr: ProcessResult) {
        debug!("read command(cid={}) finished", cid);

        let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        let cb = ctx.callback.take().unwrap();
        if let ProcessResult::NextCommand { cmd } = pr {
            self.on_receive_new_cmd(cmd, cb);
        } else {
            execute_callback(cb, pr);
        }
    }

    /// Event handler for the failure of write prepare.
    ///
    /// Write prepare failure typically means conflicting transactions are detected. Delivers the
    /// error to the callback, and releases the latches.
    fn on_write_prepare_failed(&mut self, cid: u64, e: Error) {
        debug!("write command(cid={}) failed at prewrite.", cid);

        let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        let cb = ctx.callback.take().unwrap();
        let pr = ProcessResult::Failed { err: StorageError::from(e) };
        execute_callback(cb, pr);

        self.release_lock(&ctx.lock, cid);
    }

    /// Event handler for the success of write prepare.
    ///
    /// Initiates an async write operation on the storage engine, there'll be a `WriteFinished`
    /// message when it finishes.
    fn on_write_prepare_finished(&mut self,
                                 cid: u64,
                                 cmd: Command,
                                 pr: ProcessResult,
                                 to_be_write: Vec<Modify>) {
        if let Err(e) = {
            let engine_cb = make_engine_cb(cid, pr, self.schedch.clone());
            self.engine.async_write(extract_ctx(&cmd), to_be_write, engine_cb)
        } {
            let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
            assert_eq!(ctx.cid, cid);
            let cb = ctx.callback.take().unwrap();
            execute_callback(cb, ProcessResult::Failed { err: StorageError::from(e) });

            self.release_lock(&ctx.lock, cid);
        }
    }

    /// Event handler for the success of write.
    fn on_write_finished(&mut self, cid: u64, pr: ProcessResult, result: EngineResult<()>) {
        debug!("write finished for command, cid={}", cid);
        let mut ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        let cb = ctx.callback.take().unwrap();
        let pr = match result {
            Ok(()) => pr,
            Err(e) => ProcessResult::Failed { err: ::storage::Error::from(e) },
        };
        if let ProcessResult::NextCommand { cmd } = pr {
            self.on_receive_new_cmd(cmd, cb);
        } else {
            execute_callback(cb, pr);
        }

        self.release_lock(&ctx.lock, cid);
    }

    /// Releases all the latches held by a command.
    fn release_lock(&mut self, lock: &Lock, cid: u64) {
        let wakeup_list = self.latches.release(lock, cid);
        for wcid in wakeup_list {
            self.wakeup_cmd(wcid);
        }
    }

    /// Wakes up the next command for execution.
    fn wakeup_cmd(&mut self, cid: u64) {
        if self.acquire_lock(cid) {
            self.get_snapshot(cid);
        }
    }

    /// Shuts down the event loop.
    fn shutdown(&mut self, event_loop: &mut EventLoop<Self>) {
        info!("receive shutdown command");
        event_loop.shutdown();
    }
}

/// Registers a timer.
fn register_timer(event_loop: &mut EventLoop<Scheduler>,
                  tick: Tick,
                  delay: u64)
                  -> Result<mio::Timeout> {
    event_loop.timeout(tick, Duration::from_millis(delay))
        .map_err(|e| box_err!("register timer err: {:?}", e))
}

/// Handler of the scheduler event loop.
impl mio::Handler for Scheduler {
    type Timeout = Tick;
    type Message = Msg;

    /// Handler for timeout events.
    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
        match timeout {
            Tick::ReportStatistic => self.on_report_staticstic_tick(event_loop),
        }
    }

    /// Event handler for message events.
    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::Quit => self.shutdown(event_loop),
            Msg::RawCmd { cmd, cb } => self.on_receive_new_cmd(cmd, cb),
            Msg::SnapshotFinished { cid, snapshot } => self.on_snapshot_finished(cid, snapshot),
            Msg::ReadFinished { cid, pr } => self.on_read_finished(cid, pr),
            Msg::WritePrepareFinished { cid, cmd, pr, to_be_write } => {
                self.on_write_prepare_finished(cid, cmd, pr, to_be_write)
            }
            Msg::WritePrepareFailed { cid, err } => self.on_write_prepare_failed(cid, err),
            Msg::WriteFinished { cid, pr, result } => self.on_write_finished(cid, pr, result),
        }
    }

    /// Handler for tick events.
    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            // stop work threads if has
        }
    }
}
