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

use std::sync::{self, Arc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::RefCell;
use std::error;
use std::time::Instant;
use std::mem;

use rocksdb::{DB, WriteBatch, Writable};
use protobuf::Message;

use kvproto::metapb;
use kvproto::eraftpb::{Entry, Snapshot, ConfState, HardState};
use kvproto::raft_serverpb::{RaftSnapshotData, RaftLocalState, RegionLocalState, RaftApplyState,
                             PeerState};
use util::HandyRwLock;
use util::codec::bytes::BytesEncoder;
use util::worker::Scheduler;
use util::rocksdb;
use raft::{self, Storage, RaftState, StorageError, Error as RaftError, Ready};
use raftstore::{Result, Error};
use super::worker::SnapTask;
use super::keys::{self, enc_start_key, enc_end_key};
use super::engine::{Snapshot as DbSnapshot, Peekable, Iterable, Mutable, delete_all_in_range};
use super::{SnapFile, SnapKey, SnapEntry, SnapManager};
use storage::CF_RAFT;

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: usize = 5;

pub type Ranges = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(PartialEq, Debug)]
pub enum SnapState {
    Relax,
    Generating,
    Snap(Snapshot),
    Applying,
    Failed,
}

pub struct PeerStorage {
    pub engine: Arc<DB>,

    pub region: metapb::Region,
    pub raft_state: RaftLocalState,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,

    snap_state: RefCell<SnapState>,
    snap_sched: Scheduler<SnapTask>,
    snap_tried_cnt: AtomicUsize,

    pub tag: String,
}

fn storage_error<E>(error: E) -> raft::Error
    where E: Into<Box<error::Error + Send + Sync>>
{
    raft::Error::Store(StorageError::Other(error.into()))
}

impl From<Error> for RaftError {
    fn from(err: Error) -> RaftError {
        storage_error(err)
    }
}

impl<T> From<sync::PoisonError<T>> for RaftError {
    fn from(_: sync::PoisonError<T>) -> RaftError {
        storage_error("lock failed")
    }
}

pub struct ApplySnapResult {
    // prev_region is the region before snapshot applied.
    pub prev_region: metapb::Region,
    pub region: metapb::Region,
}

pub struct InvokeContext {
    pub raft_state: RaftLocalState,
    pub apply_state: RaftApplyState,
    pub wb: WriteBatch,
    engine: Arc<DB>,
}

impl InvokeContext {
    pub fn new(store: &PeerStorage) -> InvokeContext {
        InvokeContext {
            raft_state: store.raft_state.clone(),
            apply_state: store.apply_state.clone(),
            wb: WriteBatch::new(),
            engine: store.engine.clone(),
        }
    }

    pub fn save_raft(&self, region_id: u64) -> Result<()> {
        let raft_cf = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        try!(self.wb.put_msg_cf(*raft_cf, &keys::raft_state_key(region_id), &self.raft_state));
        Ok(())
    }

    pub fn save_apply(&self, region_id: u64) -> Result<()> {
        let raft_cf = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        try!(self.wb.put_msg_cf(*raft_cf,
                                &keys::apply_state_key(region_id),
                                &self.apply_state));
        Ok(())
    }
}

impl PeerStorage {
    pub fn new(engine: Arc<DB>,
               region: &metapb::Region,
               snap_sched: Scheduler<SnapTask>,
               tag: String)
               -> Result<PeerStorage> {
        debug!("creating storage on {} for {:?}", engine.path(), region);
        let raft_state =
            match try!(engine.get_msg_cf(CF_RAFT, &keys::raft_state_key(region.get_id()))) {
                Some(s) => s,
                None => {
                    let mut raft_state = RaftLocalState::new();
                    if !region.get_peers().is_empty() {
                        raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
                    }
                    raft_state
                }
            };
        let apply_state =
            match try!(engine.get_msg_cf(CF_RAFT, &keys::apply_state_key(region.get_id()))) {
                Some(s) => s,
                None => {
                    let mut apply_state = RaftApplyState::new();
                    if !region.get_peers().is_empty() {
                        apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
                        let state = apply_state.mut_truncated_state();
                        state.set_index(RAFT_INIT_LOG_INDEX);
                        state.set_term(RAFT_INIT_LOG_TERM);
                    }
                    apply_state
                }
            };

        Ok(PeerStorage {
            engine: engine,
            region: region.clone(),
            raft_state: raft_state,
            apply_state: apply_state,
            snap_state: RefCell::new(SnapState::Relax),
            snap_sched: snap_sched,
            snap_tried_cnt: AtomicUsize::new(0),
            tag: tag,
            applied_index_term: RAFT_INIT_LOG_TERM,
        })
    }

    pub fn is_initialized(&self) -> bool {
        !self.region.get_peers().is_empty()
    }

    pub fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.raft_state.get_hard_state().clone();
        let mut conf_state = ConfState::new();
        if hard_state == HardState::new() {
            assert!(!self.is_initialized(),
                    "peer for region {:?} is initialized but local state {:?} has empty hard \
                     state",
                    self.region,
                    self.raft_state);

            return Ok(RaftState {
                hard_state: hard_state,
                conf_state: conf_state,
            });
        }

        for p in self.region.get_peers() {
            conf_state.mut_nodes().push(p.get_id());
        }

        Ok(RaftState {
            hard_state: hard_state,
            conf_state: conf_state,
        })
    }

    fn check_range(&self, low: u64, high: u64) -> raft::Result<()> {
        if low > high {
            return Err(storage_error(format!("low: {} is greater that high: {}", low, high)));
        } else if low <= self.truncated_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        } else if high > self.last_index() + 1 {
            return Err(storage_error(format!("entries' high {} is out of bound lastindex {}",
                                             high,
                                             self.last_index())));
        }
        Ok(())
    }

    pub fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        try!(self.check_range(low, high));
        let mut ents = Vec::with_capacity((high - low) as usize);
        let mut total_size: u64 = 0;
        let mut next_index = low;
        let mut exceeded_max_size = false;

        let start_key = keys::raft_log_key(self.get_region_id(), low);
        let end_key = keys::raft_log_key(self.get_region_id(), high);

        try!(self.engine.scan_cf(CF_RAFT,
                                 &start_key,
                                 &end_key,
                                 &mut |_, value| {
            let mut entry = Entry::new();
            try!(entry.merge_from_bytes(value));

            // May meet gap or has been compacted.
            if entry.get_index() != next_index {
                return Ok(false);
            }

            next_index += 1;

            total_size += entry.compute_size() as u64;
            exceeded_max_size = total_size > max_size;

            if !exceeded_max_size || ents.is_empty() {
                ents.push(entry);
            }

            Ok(!exceeded_max_size)
        }));

        // If we get the correct number of entries the total size exceeds max_size, returns.
        if ents.len() == (high - low) as usize || exceeded_max_size {
            return Ok(ents);
        }

        // Here means we don't fetch enough entries.
        Err(RaftError::Store(StorageError::Unavailable))
    }

    pub fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }
        try!(self.check_range(idx, idx + 1));
        let key = keys::raft_log_key(self.get_region_id(), idx);
        match try!(self.engine.get_msg_cf::<Entry>(CF_RAFT, &key)) {
            Some(entry) => Ok(entry.get_term()),
            None => Err(RaftError::Store(StorageError::Unavailable)),
        }
    }

    #[inline]
    pub fn first_index(&self) -> u64 {
        self.apply_state.get_truncated_state().get_index() + 1
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.raft_state.get_last_index()
    }

    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.apply_state.get_applied_index()
    }

    #[inline]
    pub fn truncated_index(&self) -> u64 {
        self.apply_state.get_truncated_state().get_index()
    }

    #[inline]
    pub fn truncated_term(&self) -> u64 {
        self.apply_state.get_truncated_state().get_term()
    }

    pub fn get_region(&self) -> &metapb::Region {
        &self.region
    }

    pub fn raw_snapshot(&self) -> DbSnapshot {
        DbSnapshot::new(self.engine.clone())
    }

    pub fn snapshot(&self) -> raft::Result<Snapshot> {
        let mut snap_state = self.snap_state.borrow_mut();

        if let SnapState::Snap(_) = *snap_state {
            match mem::replace(&mut *snap_state, SnapState::Relax) {
                SnapState::Snap(s) => return Ok(s),
                _ => unreachable!(),
            }
        }

        if SnapState::Relax == *snap_state {
            info!("{} requesting snapshot...", self.tag);
            self.snap_tried_cnt.store(0, Ordering::Relaxed);
            *snap_state = SnapState::Generating;
        } else if SnapState::Failed == *snap_state {
            let mut snap_tried_cnt = self.snap_tried_cnt.load(Ordering::Relaxed);
            if snap_tried_cnt >= MAX_SNAP_TRY_CNT {
                return Err(raft::Error::Store(box_err!("failed to get snapshot after {} times",
                                                       snap_tried_cnt)));
            }
            snap_tried_cnt += 1;
            warn!("{} snapshot generating failed, retry {} time",
                  self.tag,
                  snap_tried_cnt);
            self.snap_tried_cnt.store(snap_tried_cnt, Ordering::Relaxed);
            *snap_state = SnapState::Generating;
        } else {
            return Err(raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable));
        }
        let task = SnapTask::Gen { region_id: self.get_region_id() };
        if let Err(e) = self.snap_sched.schedule(task) {
            error!("{} failed to schedule task snap generation: {:?}",
                   self.tag,
                   e);
            *snap_state = SnapState::Failed;
        }
        Err(raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable))
    }

    // Append the given entries to the raft log using previous last index or self.last_index.
    // Return the new last index for later update. After we commit in engine, we can set last_index
    // to the return one.
    pub fn append(&self, ctx: &mut InvokeContext, entries: &[Entry]) -> Result<u64> {
        debug!("{} append {} entries", self.tag, entries.len());
        let prev_last_index = ctx.raft_state.get_last_index();
        if entries.len() == 0 {
            return Ok(prev_last_index);
        }

        let handle = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        for entry in entries {
            try!(ctx.wb.put_msg_cf(*handle,
                                   &keys::raft_log_key(self.get_region_id(), entry.get_index()),
                                   entry));
        }

        let last_index = entries[entries.len() - 1].get_index();

        // Delete any previously appended log entries which never committed.
        for i in (last_index + 1)..(prev_last_index + 1) {
            try!(ctx.wb.delete_cf(*handle, &keys::raft_log_key(self.get_region_id(), i)));
        }

        ctx.raft_state.set_last_index(last_index);

        Ok(last_index)
    }

    // Apply the peer with given snapshot.
    pub fn apply_snapshot(&self,
                          ctx: &mut InvokeContext,
                          snap: &Snapshot)
                          -> Result<ApplySnapResult> {
        info!("{} begin to apply snapshot", self.tag);

        let mut snap_data = RaftSnapshotData::new();
        try!(snap_data.merge_from_bytes(snap.get_data()));

        let region_id = self.get_region_id();

        let region = snap_data.get_region();
        if region.get_id() != region_id {
            return Err(box_err!("mismatch region id {} != {}", region_id, region.get_id()));
        }

        if self.is_initialized() {
            // we can only delete the old data when the peer is initialized.
            try!(self.clear_meta(&ctx.wb));
        }

        try!(write_peer_state(&ctx.wb, region, PeerState::Applying));

        let last_index = snap.get_metadata().get_index();

        ctx.raft_state.set_last_index(last_index);
        ctx.apply_state.set_applied_index(last_index);

        // The snapshot only contains log which index > applied index, so
        // here the truncate state's (index, term) is in snapshot metadata.
        ctx.apply_state.mut_truncated_state().set_index(last_index);
        ctx.apply_state.mut_truncated_state().set_term(snap.get_metadata().get_term());

        info!("{} apply snapshot for region {:?} with state {:?} ok",
              self.tag,
              region,
              ctx.apply_state);

        Ok(ApplySnapResult {
            prev_region: self.region.clone(),
            region: region.clone(),
        })
    }

    // Discard all log entries prior to compact_index. We must guarantee
    // that the compact_index is not greater than applied index.
    pub fn compact(&self, state: &mut RaftApplyState, compact_index: u64) -> Result<()> {
        debug!("{} compact log entries to prior to {}",
               self.tag,
               compact_index);

        if compact_index <= self.truncated_index() {
            return Err(box_err!("try to truncate compacted entries"));
        } else if compact_index > self.applied_index() {
            return Err(box_err!("compact index {} > applied index {}",
                                compact_index,
                                self.applied_index()));
        }

        let term = try!(self.term(compact_index - 1));
        // we don't actually compact the log now, we add an async task to do it.

        state.mut_truncated_state().set_index(compact_index - 1);
        state.mut_truncated_state().set_term(term);

        Ok(())
    }

    /// Delete all meta belong to the region. Results are stored in `wb`.
    pub fn clear_meta(&self, wb: &WriteBatch) -> Result<()> {
        let region_id = self.get_region_id();
        let (meta_start, meta_end) = (keys::region_meta_prefix(region_id),
                                      keys::region_meta_prefix(region_id + 1));
        try!(self.engine.scan(&meta_start,
                              &meta_end,
                              &mut |key, _| {
                                  try!(wb.delete(key));
                                  Ok(true)
                              }));

        let handle = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        let (raft_start, raft_end) = (keys::region_raft_prefix(region_id),
                                      keys::region_raft_prefix(region_id + 1));
        try!(self.engine.scan_cf(CF_RAFT,
                                 &raft_start,
                                 &raft_end,
                                 &mut |key, _| {
                                     try!(wb.delete_cf(*handle, key));
                                     Ok(true)
                                 }));

        Ok(())
    }

    /// Delete all data belong to the region.
    /// If return Err, data may get partial deleted.
    pub fn clear_data(&self) -> Result<()> {
        let timer = Instant::now();
        let (start_key, end_key) = (enc_start_key(self.get_region()),
                                    enc_end_key(self.get_region()));

        try!(delete_all_in_range(&self.engine, &start_key, &end_key));
        info!("{} clean peer data takes {:?}", self.tag, timer.elapsed());
        Ok(())
    }

    pub fn get_engine(&self) -> Arc<DB> {
        self.engine.clone()
    }

    #[inline]
    pub fn is_applying_snap(&self) -> bool {
        self.is_snap_state(SnapState::Applying)
    }

    #[inline]
    pub fn set_snap_state(&mut self, state: SnapState) {
        *self.snap_state.borrow_mut() = state
    }

    #[inline]
    pub fn is_snap_state(&self, state: SnapState) -> bool {
        *self.snap_state.borrow() == state
    }

    pub fn get_region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub fn handle_raft_ready(&mut self, ready: &Ready) -> Result<Option<ApplySnapResult>> {
        let mut ctx = InvokeContext::new(self);
        let mut apply_snap_res = None;
        let region_id = self.get_region_id();
        if !raft::is_empty_snap(&ready.snapshot) {
            let res = try!(self.apply_snapshot(&mut ctx, &ready.snapshot));
            apply_snap_res = Some(res);
        }
        if !ready.entries.is_empty() {
            try!(self.append(&mut ctx, &ready.entries));
        }

        // Last index is 0 means the peer is created from raft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if ctx.raft_state.get_last_index() > 0 {
            if let Some(ref hs) = ready.hs {
                ctx.raft_state.set_hard_state(hs.clone());
            }
        }

        if ctx.raft_state != self.raft_state {
            try!(ctx.save_raft(region_id));
        }

        if ctx.apply_state != self.apply_state {
            try!(ctx.save_apply(region_id));
        }

        if !ctx.wb.is_empty() {
            try!(self.engine.write(ctx.wb));
        }

        self.raft_state = ctx.raft_state;
        self.apply_state = ctx.apply_state;
        // If we apply snapshot ok, we should update some infos like applied index too.
        if let Some(res) = apply_snap_res {
            self.set_snap_state(SnapState::Applying);

            // cleanup data before schedule apply task
            if self.is_initialized() {
                if let Err(e) = self.clear_data() {
                    // No need panic here, when applying snapshot, the deletion will be tried
                    // again. But if the region range changes, like [a, c) -> [a, b) and [b, c),
                    // [b, c) will be kept in rocksdb until a covered snapshot is applied or
                    // store is restarted.
                    error!("{} cleanup data fail, may leave some dirty data: {:?}",
                           self.tag,
                           e);
                }
            }

            let task = SnapTask::Apply { region_id: region_id };
            // TODO: gracefully remove region instead.
            self.snap_sched.schedule(task).expect("snap apply job should not fail");
            self.region = res.region.clone();
            return Ok(Some(res));
        }

        Ok(None)
    }
}

fn build_snap_file(f: &mut SnapFile,
                   snap: &DbSnapshot,
                   region: &metapb::Region)
                   -> raft::Result<()> {
    let mut snap_size = 0;
    let mut snap_key_cnt = 0;
    let (begin_key, end_key) = (enc_start_key(region), enc_end_key(region));
    for cf in snap.cf_names() {
        box_try!(f.encode_compact_bytes(cf.as_bytes()));
        try!(snap.scan_cf(cf,
                          &begin_key,
                          &end_key,
                          &mut |key, value| {
            snap_size += key.len();
            snap_size += value.len();
            snap_key_cnt += 1;
            try!(f.encode_compact_bytes(key));
            try!(f.encode_compact_bytes(value));
            Ok(true)
        }));
        // use an empty byte array to indicate that cf reaches an end.
        box_try!(f.encode_compact_bytes(b""));
    }
    // use an empty byte array to indicate that kvpair reaches an end.
    box_try!(f.encode_compact_bytes(b""));
    try!(f.save());

    info!("[region {}] scan snapshot, size {}, key count {}",
          region.get_id(),
          snap_size,
          snap_key_cnt);
    Ok(())
}

pub fn do_snapshot(mgr: SnapManager, snap: &DbSnapshot, region_id: u64) -> raft::Result<Snapshot> {
    debug!("[region {}] begin to generate a snapshot", region_id);

    let apply_state: RaftApplyState =
        match try!(snap.get_msg_cf(CF_RAFT, &keys::apply_state_key(region_id))) {
            None => return Err(box_err!("could not load raft state of region {}", region_id)),
            Some(state) => state,
        };

    let idx = apply_state.get_applied_index();
    let term = if idx == apply_state.get_truncated_state().get_index() {
        apply_state.get_truncated_state().get_term()
    } else {
        match try!(snap.get_msg_cf::<Entry>(CF_RAFT, &keys::raft_log_key(region_id, idx))) {
            None => return Err(box_err!("entry {} of {} not found.", idx, region_id)),
            Some(entry) => entry.get_term(),
        }
    };

    let key = SnapKey::new(region_id, term, idx);

    mgr.wl().register(key.clone(), SnapEntry::Generating);
    defer!(mgr.wl().deregister(&key, &SnapEntry::Generating));

    let mut state: RegionLocalState = try!(snap.get_msg(&keys::region_state_key(key.region_id))
        .and_then(|res| {
            match res {
                None => Err(box_err!("could not find region info")),
                Some(state) => Ok(state),
            }
        }));

    if state.get_state() != PeerState::Normal {
        return Err(box_err!("snap job for {} seems stale, skip.", region_id));
    }

    let mut snapshot = Snapshot::new();

    // Set snapshot metadata.
    snapshot.mut_metadata().set_index(key.idx);
    snapshot.mut_metadata().set_term(key.term);

    let mut conf_state = ConfState::new();
    for p in state.get_region().get_peers() {
        conf_state.mut_nodes().push(p.get_id());
    }

    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut snap_file = try!(mgr.rl().get_snap_file(&key, true));
    if snap_file.exists() {
        if let Err(e) = snap_file.validate() {
            error!("[region {}] file {} is invalid, will regenerate: {:?}",
                   region_id,
                   snap_file.path().display(),
                   e);
            try!(snap_file.try_delete());
            try!(snap_file.init());
            try!(build_snap_file(&mut snap_file, snap, state.get_region()));
        }
    } else {
        try!(build_snap_file(&mut snap_file, snap, state.get_region()));
    }

    // Set snapshot data.
    let mut snap_data = RaftSnapshotData::new();
    snap_data.set_region(state.take_region());

    let len = try!(snap_file.meta()).len();
    snap_data.set_file_size(len);

    let mut v = vec![];
    box_try!(snap_data.write_to_vec(&mut v));
    snapshot.set_data(v);

    Ok(snapshot)
}

// When we bootstrap the region or handling split new region, we must
// call this to initialize region local state first.
pub fn write_initial_state<T: Mutable>(engine: &DB, w: &T, region_id: u64) -> Result<()> {
    let mut raft_state = RaftLocalState::new();
    raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
    raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
    raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);

    let mut apply_state = RaftApplyState::new();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state.mut_truncated_state().set_index(RAFT_INIT_LOG_INDEX);
    apply_state.mut_truncated_state().set_term(RAFT_INIT_LOG_TERM);

    let raft_cf = try!(rocksdb::get_cf_handle(engine, CF_RAFT));
    try!(w.put_msg_cf(*raft_cf, &keys::raft_state_key(region_id), &raft_state));
    try!(w.put_msg_cf(*raft_cf, &keys::apply_state_key(region_id), &apply_state));

    Ok(())
}

pub fn write_peer_state<T: Mutable>(w: &T,
                                    region: &metapb::Region,
                                    state: PeerState)
                                    -> Result<()> {
    let region_id = region.get_id();
    let mut region_state = RegionLocalState::new();
    region_state.set_state(state);
    region_state.set_region(region.clone());
    try!(w.put_msg(&keys::region_state_key(region_id), &region_state));
    Ok(())
}

impl Storage for PeerStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.initial_state()
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        self.entries(low, high, max_size)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.term(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index())
    }

    fn snapshot(&self) -> raft::Result<Snapshot> {
        self.snapshot()
    }
}

#[cfg(test)]
mod test {
    use std::sync::*;
    use std::sync::mpsc::*;
    use std::io;
    use std::fs::File;
    use kvproto::eraftpb::{Entry, ConfState};
    use kvproto::raft_serverpb::RaftSnapshotData;
    use raft::{StorageError, Error as RaftError};
    use tempdir::*;
    use protobuf;
    use raftstore;
    use raftstore::store::*;
    use raftstore::store::worker::{SnapRunner, MsgSender};
    use util::codec::number::NumberEncoder;
    use raftstore::store::worker::SnapTask;
    use util::worker::{Worker, Scheduler};
    use util::HandyRwLock;
    use util::rocksdb::new_engine;
    use storage::{CF_DEFAULT, CF_RAFT};

    use super::InvokeContext;

    impl MsgSender for Sender<Msg> {
        fn send(&self, msg: Msg) -> raftstore::Result<()> {
            Sender::send(self, msg).unwrap();
            Ok(())
        }
    }

    fn new_storage(sched: Scheduler<SnapTask>, path: &TempDir) -> PeerStorage {
        let db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT, CF_RAFT]).unwrap();
        let db = Arc::new(db);
        bootstrap::bootstrap_store(&db, 1, 1).expect("");
        let region = bootstrap::bootstrap_region(&db, 1, 1, 1).expect("");
        PeerStorage::new(db, &region, sched, "".to_owned()).unwrap()
    }

    fn new_storage_from_ents(sched: Scheduler<SnapTask>,
                             path: &TempDir,
                             ents: &[Entry])
                             -> PeerStorage {
        let mut store = new_storage(sched, path);
        let mut ctx = InvokeContext::new(&store);
        store.append(&mut ctx, &ents[1..]).expect("");
        ctx.apply_state.mut_truncated_state().set_index(ents[0].get_index());
        ctx.apply_state.mut_truncated_state().set_term(ents[0].get_term());
        ctx.apply_state.set_applied_index(ents.last().unwrap().get_index());
        ctx.save_apply(store.get_region_id()).unwrap();
        store.engine.write(ctx.wb).expect("");
        store.raft_state = ctx.raft_state;
        store.apply_state = ctx.apply_state;
        store
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::new();
        e.set_index(index);
        e.set_term(term);
        e
    }

    fn size_of<T: protobuf::Message>(m: &T) -> u32 {
        m.compute_size()
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
        ];

        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
        ];
        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let worker = Worker::new("snap_manager");
            let sched = worker.scheduler();
            let store = new_storage_from_ents(sched, &td, &ents);
            let t = store.term(idx);
            if wterm != t {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];
        let max_u64 = u64::max_value();
        let mut tests = vec![
            (2, 6, max_u64, Err(RaftError::Store(StorageError::Compacted))),
            (3, 4, max_u64, Err(RaftError::Store(StorageError::Compacted))),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (4, 7, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)])),
            // even if maxsize is zero, the first entry should be returned
            (4, 7, 0, Ok(vec![new_entry(4, 4)])),
            // limit to 2
            (4, 7, (size_of(&ents[1]) + size_of(&ents[2])) as u64,
             Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (4, 7, (size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2) as u64,
             Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (4, 7, (size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1) as u64,
             Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            // all
            (4, 7, (size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])) as u64,
             Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)])),
        ];

        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let worker = Worker::new("snap_manager");
            let sched = worker.scheduler();
            let store = new_storage_from_ents(sched, &td, &ents);
            let e = store.entries(lo, hi, maxsize);
            if e != wentries {
                panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
            }
        }
    }

    // last_index and first_index are not mutated by PeerStorage on its own,
    // so we don't test them here.

    #[test]
    fn test_storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Err(RaftError::Store(StorageError::Compacted))),
            (4, Ok(())),
            (5, Ok(())),
        ];
        for (i, (idx, werr)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let worker = Worker::new("snap_manager");
            let sched = worker.scheduler();
            let store = new_storage_from_ents(sched, &td, &ents);
            let mut ctx = InvokeContext::new(&store);
            let res = store.compact(&mut ctx.apply_state, idx);
            // TODO check exact error type after refactoring error.
            if res.is_err() ^ werr.is_err() {
                panic!("#{}: want {:?}, got {:?}", i, werr, res);
            }
            if res.is_ok() {
                store.engine.write(ctx.wb).expect("");
            }
        }
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2, 3]);

        let td = TempDir::new("tikv-store-test").unwrap();
        let snap_dir = TempDir::new("snap_dir").unwrap();
        let mgr = new_snap_mgr(snap_dir.path().to_str().unwrap(), None);
        let mut worker = Worker::new("snap_manager");
        let sched = worker.scheduler();
        let mut s = new_storage_from_ents(sched, &td, &ents);
        let (tx, rx) = channel();
        let runner = SnapRunner::new(s.engine.clone(), tx, mgr);
        worker.start(runner).unwrap();
        let snap = s.snapshot();
        let unavailable = RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);
        assert_eq!(snap.unwrap_err(), unavailable);
        assert!(s.is_snap_state(SnapState::Generating));

        let snap = match rx.recv().unwrap() {
            Msg::SnapGenRes { snap, .. } => snap.unwrap(),
            m => panic!("unexpected snap: {:?}", m),
        };
        assert_eq!(snap.get_metadata().get_index(), 5);
        assert_eq!(snap.get_metadata().get_term(), 5);
        assert!(!snap.get_data().is_empty());

        let mut data = RaftSnapshotData::new();
        protobuf::Message::merge_from_bytes(&mut data, snap.get_data()).expect("");
        assert_eq!(data.get_region().get_id(), 1);
        assert_eq!(data.get_region().get_peers().len(), 1);

        s.set_snap_state(SnapState::Snap(snap.clone()));
        assert_eq!(s.snapshot(), Ok(snap));
    }

    #[test]
    fn test_storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                vec![new_entry(4, 4), new_entry(5, 5)],
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                vec![new_entry(4, 6), new_entry(5, 6)],
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
            ),
            // truncate incoming entries, truncate the existing entries and append
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                vec![new_entry(4, 5)],
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(4, 5)],
                vec![new_entry(4, 5)],
            ),
            // direct append
            (
                vec![new_entry(6, 5)],
                vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
            ),
        ];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let td = TempDir::new("tikv-store-test").unwrap();
            let worker = Worker::new("snap_manager");
            let sched = worker.scheduler();
            let mut store = new_storage_from_ents(sched, &td, &ents);
            let mut ctx = InvokeContext::new(&store);
            store.append(&mut ctx, &entries).expect("");
            store.engine.write(ctx.wb).expect("");
            store.raft_state = ctx.raft_state;
            let li = store.last_index();
            let actual_entries = store.entries(4, li + 1, u64::max_value()).expect("");
            if actual_entries != wentries {
                panic!("#{}: want {:?}, got {:?}", i, wentries, actual_entries);
            }
        }
    }

    #[test]
    fn test_storage_apply_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::new();
        cs.set_nodes(vec![1, 2, 3]);

        let td1 = TempDir::new("tikv-store-test").unwrap();
        let snap_dir = TempDir::new("snap").unwrap();
        let mgr = new_snap_mgr(snap_dir.path().to_str().unwrap(), None);
        let mut worker = Worker::new("snap_manager");
        let sched = worker.scheduler();
        let s1 = new_storage_from_ents(sched.clone(), &td1, &ents);
        let (tx, rx) = channel();
        let runner = SnapRunner::new(s1.engine.clone(), tx, mgr.clone());
        worker.start(runner).unwrap();
        assert!(s1.snapshot().is_err());
        let snap1 = match rx.recv().unwrap() {
            Msg::SnapGenRes { snap, .. } => snap.unwrap(),
            m => panic!("unexpected message: {:?}", m),
        };
        assert_eq!(s1.truncated_index(), 3);
        assert_eq!(s1.truncated_term(), 3);

        let key = SnapKey::from_snap(&snap1).unwrap();
        let source_snap = mgr.rl().get_snap_file(&key, true).unwrap();
        let mut dst_snap = mgr.rl().get_snap_file(&key, false).unwrap();
        let mut f = File::open(source_snap.path()).unwrap();
        dst_snap.encode_u64(0).unwrap();
        io::copy(&mut f, &mut dst_snap).unwrap();
        dst_snap.save().unwrap();

        let td2 = TempDir::new("tikv-store-test").unwrap();
        let s2 = new_storage(sched, &td2);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
        let mut ctx = InvokeContext::new(&s2);
        s2.apply_snapshot(&mut ctx, &snap1).unwrap();
        assert_eq!(ctx.apply_state.get_applied_index(), 5);
        assert_eq!(ctx.raft_state.get_last_index(), 5);
        assert_eq!(ctx.apply_state.get_truncated_state().get_index(), 5);
        assert_eq!(ctx.apply_state.get_truncated_state().get_term(), 5);
        assert_eq!(s2.first_index(), s2.applied_index() + 1);
    }
}
