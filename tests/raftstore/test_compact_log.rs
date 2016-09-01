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

use std::collections::HashMap;

use tikv::raftstore::store::*;
use tikv::storage::CF_RAFT;
use tikv::util::rocksdb::get_cf_handle;
use rocksdb::DB;
use protobuf;
use kvproto::raft_serverpb::RaftApplyState;

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;

fn get_msg_cf_or_default<M>(engine: &DB, cf: &str, key: &[u8]) -> M
    where M: protobuf::Message + protobuf::MessageStatic
{
    engine.get_msg_cf(cf, key).unwrap().unwrap_or_default()
}

fn test_compact_log<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let mut before_states = HashMap::new();

    for (&id, engine) in &cluster.engines {
        let mut state: RaftApplyState =
            get_msg_cf_or_default(engine, CF_RAFT, &keys::apply_state_key(1));
        before_states.insert(id, state.take_truncated_state());
    }

    for i in 1..1000 {
        let (k, v) = (format!("key{}", i), format!("value{}", i));
        let key = k.as_bytes();
        let value = v.as_bytes();
        cluster.must_put(key, value);
        let v = cluster.get(key);
        assert_eq!(v, Some(value.to_vec()));
    }

    // wait log gc.
    sleep_ms(500);

    // Every peer must have compacted logs, so the truncate log state index/term must > than before.
    for (&id, engine) in &cluster.engines {
        let mut state: RaftApplyState =
            get_msg_cf_or_default(engine, CF_RAFT, &keys::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = before_states.get(&id).unwrap();
        let idx = after_state.get_index();
        assert!(idx > before_state.get_index());
        assert!(after_state.get_term() > before_state.get_term());

        let handle = get_cf_handle(engine, CF_RAFT).unwrap();
        for i in 0..idx {
            let key = keys::raft_log_key(1, i);
            assert!(engine.get_cf(*handle, &key).unwrap().is_none());
        }
    }
}

fn test_compact_limit<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_log_gc_limit = 1000;
    cluster.cfg.raft_store.raft_log_gc_threshold = 2000;
    cluster.run();

    cluster.must_put(b"k1", b"v1");

    let mut before_states = HashMap::new();

    for (&id, engine) in &cluster.engines {
        must_get_equal(engine, b"k1", b"v1");
        let mut state: RaftApplyState =
            get_msg_cf_or_default(engine, CF_RAFT, &keys::apply_state_key(1));
        let state = state.take_truncated_state();
        // compact should not start
        assert_eq!(RAFT_INIT_LOG_INDEX, state.get_index());
        assert_eq!(RAFT_INIT_LOG_TERM, state.get_term());
        before_states.insert(id, state);
    }

    for i in 1..600 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        let v2 = cluster.get(&k);
        assert_eq!(v2, Some(v));
    }

    // wait log gc.
    sleep_ms(500);

    // limit has not reached, should not gc.
    for (&id, engine) in &cluster.engines {
        let mut state: RaftApplyState =
            get_msg_cf_or_default(engine, CF_RAFT, &keys::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = before_states.get(&id).unwrap();
        let idx = after_state.get_index();
        assert_eq!(idx, before_state.get_index());
    }

    // get will use lease read so will not increase raft index
    for i in 600..1200 {
        let k = i.to_string().into_bytes();
        let v = k.clone();
        cluster.must_put(&k, &v);
        let v2 = cluster.get(&k);
        assert_eq!(v2, Some(v));
    }

    sleep_ms(500);

    // Every peer must have compacted logs, so the truncate log state index/term must > than before.
    for (&id, engine) in &cluster.engines {
        let mut state: RaftApplyState =
            get_msg_cf_or_default(engine, CF_RAFT, &keys::apply_state_key(1));
        let after_state = state.take_truncated_state();

        let before_state = before_states.get(&id).unwrap();
        let idx = after_state.get_index();
        assert!(idx > before_state.get_index());

        let handle = get_cf_handle(engine, CF_RAFT).unwrap();
        for i in 0..idx {
            let key = keys::raft_log_key(1, i);
            assert!(engine.get_cf(*handle, &key).unwrap().is_none());
        }
    }
}

#[test]
fn test_node_compact_log() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_log(&mut cluster);
}

#[test]
fn test_server_compact_log() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_compact_log(&mut cluster);
}

#[test]
fn test_node_compact_limit() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_compact_limit(&mut cluster);
}

#[test]
fn test_server_compact_limit() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_compact_limit(&mut cluster);
}
