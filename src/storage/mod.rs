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

//! Storage module.

use std::thread;
use std::boxed::FnBox;
use std::fmt::{self, Debug, Display, Formatter};
use std::error;
use std::sync::{Arc, Mutex};
use std::io::Error as IoError;
use kvproto::kvrpcpb::LockInfo;
use mio::{EventLoop, EventLoopBuilder};

pub mod engine;
pub mod mvcc;
pub mod txn;
pub mod config;
mod types;

pub use self::config::Config;
pub use self::engine::{Engine, Snapshot, Dsn, TEMP_DIR, new_engine, Modify, Cursor,
                       Error as EngineError};
pub use self::engine::raftkv::RaftKv;
pub use self::txn::{SnapshotStore, Scheduler, Msg};
pub use self::types::{Key, Value, KvPair, make_key};
pub type Callback<T> = Box<FnBox(Result<T>) + Send>;

pub type CfName = &'static str;

/// The "default" column family which stores commited value.
pub const CF_DEFAULT: CfName = "default";

/// The "lock" column family which stores the location of primary lock for an
/// uncommitted transaction.
pub const CF_LOCK: CfName = "lock";

/// The "write" column family which stores uncommitted data.
pub const CF_WRITE: CfName = "write";

pub const DEFAULT_CFS: &'static [CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];

/// Low-level key-value mutation to the underlying storage engine.
///
/// Higher-level write commands will be mapped to these mutations. Timestamp
/// is embeded in the key, hence transparent to this layer,
#[derive(Debug, Clone)]
pub enum Mutation {
    Put((Key, Value)),
    Delete(Key),
    Lock(Key),
}

#[allow(match_same_arms)]
impl Mutation {
    /// Gets the key of this mutation.
    pub fn key(&self) -> &Key {
        match *self {
            Mutation::Put((ref key, _)) => key,
            Mutation::Delete(ref key) => key,
            Mutation::Lock(ref key) => key,
        }
    }
}

use kvproto::kvrpcpb::Context;

/// Callback for receiving execution result of a command from the storage engine.
pub enum StorageCb {
    Boolean(Callback<()>),
    Booleans(Callback<Vec<Result<()>>>),
    SingleValue(Callback<Option<Value>>),
    KvPairs(Callback<Vec<Result<KvPair>>>),
    Locks(Callback<Vec<LockInfo>>),
}

/// Higher-level commands which present the abstraction of timestamped key.
///
/// Multiple commands are involved in a write transaction.
pub enum Command {
    /// Gets the row by `key` with maximum timestamp no greater than `start_ts`.
    Get {
        ctx: Context,
        key: Key,
        start_ts: u64,
    },

    /// Gets the rows by `keys`, each with maximum timestamp no greater than `start_ts`.
    BatchGet {
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
    },

    /// Scans rows from `start_key`, returns up to `limit` results, each with
    /// maximum timestamp no greater than `start_ts`.
    Scan {
        ctx: Context,
        start_key: Key,
        limit: usize,
        start_ts: u64,
    },

    /// Prewrites a transaction at `start_ts`, which essentially locks all the
    /// rows in the transaction.
    ///
    /// A prewrite contains a list of `mutations`, which will be buffered in the
    /// "write" family column of the rows before commit.
    ///
    /// One of the rows in the mutations is selected as the `primary`, who's
    /// "lock" column family is used to keep track of the transaction state; the
    /// "lock" column family of other rows will have a reference to the primary.
    ///
    /// If the mutation to the primary row is committed, then all the mutations
    /// are committed, otherwise if it is aborted, all the mutations are aborted.
    /// This is the technique to ensure the atomicity property of a transaction.
    /// If a client fails half way through in a transaction, a subsequent
    /// conflicting transaction will help it either roll back or roll forward,
    /// depending on the state recorded in the transaction's primary record.
    ///
    /// Prewrite may fail if conflicting transactions are detected.
    Prewrite {
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
    },

    /// Commits a transaction at `commit_ts` whose prewrite has done at `lock_ts`.
    ///
    /// The transaction state is atomically tracked by the "lock" family column
    /// of the primary row at `lock_ts`. Committing a transaction is essentially
    /// releasing the lock and applying the mutations buffered in the "write"
    /// family column at `lock_ts` to the "default" column family at `commit_ts`.
    Commit {
        ctx: Context,
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
    },

    /// Commits and gets.
    CommitThenGet {
        ctx: Context,
        key: Key,
        lock_ts: u64,
        commit_ts: u64,
        get_ts: u64,
    },

    Cleanup {
        ctx: Context,
        key: Key,
        start_ts: u64,
    },


    Rollback {
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
    },


    RollbackThenGet {
        ctx: Context,
        key: Key,
        lock_ts: u64,
    },

    /// Scans locks with maximum timestamp `max_ts`.
    ScanLock { ctx: Context, max_ts: u64 },

    ResolveLock {
        ctx: Context,
        start_ts: u64,
        commit_ts: Option<u64>,
    },

    Gc {
        ctx: Context,
        safe_point: u64,
        scan_key: Option<Key>,
        keys: Vec<Key>,
    },
}

/// Display for Command.
impl Display for Command {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Command::Get { ref key, start_ts, .. } => {
                write!(f, "kv::command::get {} @ {}", key, start_ts)
            }
            Command::BatchGet { ref keys, start_ts, .. } => {
                write!(f, "kv::command_batch_get {} @ {}", keys.len(), start_ts)
            }
            Command::Scan { ref start_key, limit, start_ts, .. } => {
                write!(f,
                       "kv::command::scan {}({}) @ {}",
                       start_key,
                       limit,
                       start_ts)
            }
            Command::Prewrite { ref mutations, start_ts, .. } => {
                write!(f,
                       "kv::command::prewrite mutations({}) @ {}",
                       mutations.len(),
                       start_ts)
            }
            Command::Commit { ref keys, lock_ts, commit_ts, .. } => {
                write!(f,
                       "kv::command::commit {} {} -> {}",
                       keys.len(),
                       lock_ts,
                       commit_ts)
            }
            Command::CommitThenGet { ref key, lock_ts, commit_ts, get_ts, .. } => {
                write!(f,
                       "kv::command::commit_then_get {:?} {} -> {} @ {}",
                       key,
                       lock_ts,
                       commit_ts,
                       get_ts)
            }
            Command::Cleanup { ref key, start_ts, .. } => {
                write!(f, "kv::command::cleanup {} @ {}", key, start_ts)
            }
            Command::Rollback { ref keys, start_ts, .. } => {
                write!(f,
                       "kv::command::rollback keys({}) @ {}",
                       keys.len(),
                       start_ts)
            }
            Command::RollbackThenGet { ref key, lock_ts, .. } => {
                write!(f, "kv::rollback_then_get {} @ {}", key, lock_ts)
            }
            Command::ScanLock { max_ts, .. } => write!(f, "kv::scan_lock {}", max_ts),
            Command::ResolveLock { start_ts, commit_ts, .. } => {
                write!(f, "kv::resolve_txn {} -> {:?}", start_ts, commit_ts)
            }
            Command::Gc { safe_point, ref scan_key, .. } => {
                write!(f, "kv::command::gc scan {:?} @{}", scan_key, safe_point)
            }
        }
    }
}

/// Debug for Command.
impl Debug for Command {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Returns `true` if this command is read-only, `false` otherwise.
impl Command {
    pub fn readonly(&self) -> bool {
        match *self {
            Command::Get { .. } |
            Command::BatchGet { .. } |
            Command::Scan { .. } |
            Command::ScanLock { .. } |
            Command::ResolveLock { .. } => true,
            Command::Gc { ref keys, .. } => keys.is_empty(),
            _ => false,
        }
    }
}

use util::transport::SendCh;

struct StorageHandle {
    handle: Option<thread::JoinHandle<()>>,
    event_loop: Option<EventLoop<Scheduler>>,
}

pub struct Storage {
    engine: Box<Engine>,
    sendch: SendCh<Msg>,
    handle: Arc<Mutex<StorageHandle>>,
}

impl Storage {
    pub fn from_engine(engine: Box<Engine>, config: &Config) -> Result<Storage> {
        let event_loop = try!(create_event_loop(config.sched_notify_capacity,
                                                config.sched_msg_per_tick));
        let sendch = SendCh::new(event_loop.channel());

        info!("storage {:?} started.", engine);
        Ok(Storage {
            engine: engine,
            sendch: sendch,
            handle: Arc::new(Mutex::new(StorageHandle {
                handle: None,
                event_loop: Some(event_loop),
            })),
        })
    }

    pub fn new(config: &Config) -> Result<Storage> {
        let engine = try!(engine::new_engine(Dsn::RocksDBPath(&config.path), DEFAULT_CFS));
        Storage::from_engine(engine, config)
    }

    pub fn start(&mut self, config: &Config) -> Result<()> {
        let mut handle = self.handle.lock().unwrap();
        if handle.handle.is_some() {
            return Err(box_err!("scheduler is already running"));
        }

        let engine = self.engine.clone();
        let builder = thread::Builder::new().name(thd_name!("storage-scheduler"));
        let mut el = handle.event_loop.take().unwrap();
        let sched_concurrency = config.sched_concurrency;
        let sched_worker_pool_size = config.sched_worker_pool_size;
        let ch = self.sendch.clone();
        let h = try!(builder.spawn(move || {
            let mut sched = Scheduler::new(engine, ch, sched_concurrency, sched_worker_pool_size);
            if let Err(e) = el.run(&mut sched) {
                panic!("scheduler run err:{:?}", e);
            }
            info!("scheduler stopped");
        }));
        handle.handle = Some(h);

        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let mut handle = self.handle.lock().unwrap();
        if handle.handle.is_none() {
            return Ok(());
        }

        if let Err(e) = self.sendch.send(Msg::Quit) {
            error!("send quit cmd to scheduler failed, error:{:?}", e);
            return Err(box_err!("failed to ask sched to quit: {:?}", e));
        }

        let h = handle.handle.take().unwrap();
        if let Err(e) = h.join() {
            return Err(box_err!("failed to join sched_handle, err:{:?}", e));
        }

        info!("storage {:?} closed.", self.engine);
        Ok(())
    }

    pub fn get_engine(&self) -> Box<Engine> {
        self.engine.clone()
    }

    fn send(&self, cmd: Command, cb: StorageCb) -> Result<()> {
        box_try!(self.sendch.send(Msg::RawCmd { cmd: cmd, cb: cb }));
        Ok(())
    }

    pub fn async_get(&self,
                     ctx: Context,
                     key: Key,
                     start_ts: u64,
                     callback: Callback<Option<Value>>)
                     -> Result<()> {
        let cmd = Command::Get {
            ctx: ctx,
            key: key,
            start_ts: start_ts,
        };
        try!(self.send(cmd, StorageCb::SingleValue(callback)));
        Ok(())
    }

    pub fn async_batch_get(&self,
                           ctx: Context,
                           keys: Vec<Key>,
                           start_ts: u64,
                           callback: Callback<Vec<Result<KvPair>>>)
                           -> Result<()> {
        let cmd = Command::BatchGet {
            ctx: ctx,
            keys: keys,
            start_ts: start_ts,
        };
        try!(self.send(cmd, StorageCb::KvPairs(callback)));
        Ok(())
    }

    pub fn async_scan(&self,
                      ctx: Context,
                      start_key: Key,
                      limit: usize,
                      start_ts: u64,
                      callback: Callback<Vec<Result<KvPair>>>)
                      -> Result<()> {
        let cmd = Command::Scan {
            ctx: ctx,
            start_key: start_key,
            limit: limit,
            start_ts: start_ts,
        };
        try!(self.send(cmd, StorageCb::KvPairs(callback)));
        Ok(())
    }

    pub fn async_prewrite(&self,
                          ctx: Context,
                          mutations: Vec<Mutation>,
                          primary: Vec<u8>,
                          start_ts: u64,
                          callback: Callback<Vec<Result<()>>>)
                          -> Result<()> {
        let cmd = Command::Prewrite {
            ctx: ctx,
            mutations: mutations,
            primary: primary,
            start_ts: start_ts,
        };
        try!(self.send(cmd, StorageCb::Booleans(callback)));
        Ok(())
    }

    pub fn async_commit(&self,
                        ctx: Context,
                        keys: Vec<Key>,
                        lock_ts: u64,
                        commit_ts: u64,
                        callback: Callback<()>)
                        -> Result<()> {
        let cmd = Command::Commit {
            ctx: ctx,
            keys: keys,
            lock_ts: lock_ts,
            commit_ts: commit_ts,
        };
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        Ok(())
    }

    pub fn async_commit_then_get(&self,
                                 ctx: Context,
                                 key: Key,
                                 lock_ts: u64,
                                 commit_ts: u64,
                                 get_ts: u64,
                                 callback: Callback<Option<Value>>)
                                 -> Result<()> {
        let cmd = Command::CommitThenGet {
            ctx: ctx,
            key: key,
            lock_ts: lock_ts,
            commit_ts: commit_ts,
            get_ts: get_ts,
        };
        try!(self.send(cmd, StorageCb::SingleValue(callback)));
        Ok(())
    }

    pub fn async_cleanup(&self,
                         ctx: Context,
                         key: Key,
                         start_ts: u64,
                         callback: Callback<()>)
                         -> Result<()> {
        let cmd = Command::Cleanup {
            ctx: ctx,
            key: key,
            start_ts: start_ts,
        };
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        Ok(())
    }

    pub fn async_rollback(&self,
                          ctx: Context,
                          keys: Vec<Key>,
                          start_ts: u64,
                          callback: Callback<()>)
                          -> Result<()> {
        let cmd = Command::Rollback {
            ctx: ctx,
            keys: keys,
            start_ts: start_ts,
        };
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        Ok(())
    }

    pub fn async_rollback_then_get(&self,
                                   ctx: Context,
                                   key: Key,
                                   lock_ts: u64,
                                   callback: Callback<Option<Value>>)
                                   -> Result<()> {
        let cmd = Command::RollbackThenGet {
            ctx: ctx,
            key: key,
            lock_ts: lock_ts,
        };
        try!(self.send(cmd, StorageCb::SingleValue(callback)));
        Ok(())
    }

    pub fn async_scan_lock(&self,
                           ctx: Context,
                           max_ts: u64,
                           callback: Callback<Vec<LockInfo>>)
                           -> Result<()> {
        let cmd = Command::ScanLock {
            ctx: ctx,
            max_ts: max_ts,
        };
        try!(self.send(cmd, StorageCb::Locks(callback)));
        Ok(())
    }

    pub fn async_resolve_lock(&self,
                              ctx: Context,
                              start_ts: u64,
                              commit_ts: Option<u64>,
                              callback: Callback<()>)
                              -> Result<()> {
        let cmd = Command::ResolveLock {
            ctx: ctx,
            start_ts: start_ts,
            commit_ts: commit_ts,
        };
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        Ok(())
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        let cmd = Command::Gc {
            ctx: ctx,
            safe_point: safe_point,
            scan_key: None,
            keys: vec![],
        };
        try!(self.send(cmd, StorageCb::Boolean(callback)));
        Ok(())
    }
}

impl Clone for Storage {
    fn clone(&self) -> Storage {
        Storage {
            engine: self.engine.clone(),
            sendch: self.sendch.clone(),
            handle: self.handle.clone(),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: EngineError) {
            from()
            cause(err)
            description(err.description())
        }
        Txn(err: txn::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Closed {
            description("storage is closed.")
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

pub fn create_event_loop(notify_capacity: usize,
                         messages_per_tick: usize)
                         -> Result<EventLoop<Scheduler>> {
    let mut builder = EventLoopBuilder::new();
    builder.notify_capacity(notify_capacity);
    builder.messages_per_tick(messages_per_tick);
    let el = try!(builder.build());
    Ok(el)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{channel, Sender};
    use kvproto::kvrpcpb::Context;

    fn expect_get_none(done: Sender<i32>) -> Callback<Option<Value>> {
        Box::new(move |x: Result<Option<Value>>| {
            assert_eq!(x.unwrap(), None);
            done.send(1).unwrap();
        })
    }

    fn expect_get_val(done: Sender<i32>, v: Vec<u8>) -> Callback<Option<Value>> {
        Box::new(move |x: Result<Option<Value>>| {
            assert_eq!(x.unwrap().unwrap(), v);
            done.send(1).unwrap();
        })
    }

    fn expect_ok<T>(done: Sender<i32>) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert!(x.is_ok());
            done.send(1).unwrap();
        })
    }

    fn expect_fail<T>(done: Sender<i32>) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            assert!(x.is_err());
            done.send(1).unwrap();
        })
    }

    fn expect_scan(done: Sender<i32>, pairs: Vec<Option<KvPair>>) -> Callback<Vec<Result<KvPair>>> {
        Box::new(move |rlt: Result<Vec<Result<KvPair>>>| {
            let rlt: Vec<Option<KvPair>> = rlt.unwrap()
                .into_iter()
                .map(Result::ok)
                .collect();
            assert_eq!(rlt, pairs);
            done.send(1).unwrap()
        })
    }

    #[test]
    fn test_get_put() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       100,
                       expect_get_none(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"x")],
                          100,
                          101,
                          expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       100,
                       expect_get_none(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       101,
                       expect_get_val(tx.clone(), b"100".to_vec()))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }
    #[test]
    fn test_scan() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![
            Mutation::Put((make_key(b"a"), b"aa".to_vec())),
            Mutation::Put((make_key(b"b"), b"bb".to_vec())),
            Mutation::Put((make_key(b"c"), b"cc".to_vec())),
            ],
                            b"a".to_vec(),
                            1,
                            expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"a"),make_key(b"b"),make_key(b"c"),],
                          1,
                          2,
                          expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.async_scan(Context::new(),
                        make_key(b"\x00"),
                        1000,
                        5,
                        expect_scan(tx.clone(),
                                    vec![
            Some((b"a".to_vec(), b"aa".to_vec())),
            Some((b"b".to_vec(), b"bb".to_vec())),
            Some((b"c".to_vec(), b"cc".to_vec())),
            ]))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_txn() {
        let config = Config::new();
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        let (tx, rx) = channel();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                            b"x".to_vec(),
                            100,
                            expect_ok(tx.clone()))
            .unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"y"), b"101".to_vec()))],
                            b"y".to_vec(),
                            101,
                            expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"x")],
                          100,
                          110,
                          expect_ok(tx.clone()))
            .unwrap();
        storage.async_commit(Context::new(),
                          vec![make_key(b"y")],
                          101,
                          111,
                          expect_ok(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_get(Context::new(),
                       make_key(b"x"),
                       120,
                       expect_get_val(tx.clone(), b"100".to_vec()))
            .unwrap();
        storage.async_get(Context::new(),
                       make_key(b"y"),
                       120,
                       expect_get_val(tx.clone(), b"101".to_vec()))
            .unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        storage.async_prewrite(Context::new(),
                            vec![Mutation::Put((make_key(b"x"), b"105".to_vec()))],
                            b"x".to_vec(),
                            105,
                            expect_fail(tx.clone()))
            .unwrap();
        rx.recv().unwrap();
        storage.stop().unwrap();
    }
}
