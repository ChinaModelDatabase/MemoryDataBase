use bytes::Bytes;
use jx_engine::actor::{spawn_shard_runtime, AofSink, Cmd, ShardRuntime};
use jx_engine::{system_now_ms, MaxMemoryPolicy};
use jx_proto::RespValue;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn rt(nshards: usize) -> ShardRuntime {
    spawn_shard_runtime(nshards, 1024, system_now_ms)
}

// --- Brief Step 1: exclusive ownership ---

#[test]
fn exclusive_shard_runtime_set_get() {
    let rt = spawn_shard_runtime(4, 1024, system_now_ms);
    assert_eq!(rt.exec(vec![b("SET"), b("a"), b("1")]), RespValue::ok());
    assert_eq!(rt.exec(vec![b("GET"), b("a")]), RespValue::bulk(b("1")));
}

#[test]
fn maxmemory_config_broadcast() {
    let rt = spawn_shard_runtime(2, 64, system_now_ms);
    rt.set_maxmemory(100, MaxMemoryPolicy::NoEviction)
        .expect("set_maxmemory on live shards");
    // First write succeeds (shard mem was 0 < 100); config broadcast applied
    // maxmemory to both shards. Second write OOMs once mem >= limit.
    assert_eq!(
        rt.exec(vec![b("SET"), b("big"), Bytes::from(vec![b'x'; 200])]),
        RespValue::ok()
    );
    let err = rt.exec(vec![b("SET"), b("big"), Bytes::from(vec![b'x'; 200])]);
    assert!(
        matches!(&err, RespValue::Error(m) if m.starts_with("OOM")),
        "got {err:?}"
    );
}

// --- Equivalents of existing runtime tests ---

#[test]
fn shard_runtime_ping_set_get() {
    let rt = rt(4);
    assert_eq!(
        rt.exec(vec![b("PING")]),
        RespValue::SimpleString("PONG".into())
    );
    assert_eq!(rt.exec(vec![b("SET"), b("a"), b("1")]), RespValue::ok());
    assert_eq!(rt.exec(vec![b("GET"), b("a")]), RespValue::bulk(b("1")));
    assert_eq!(rt.exec(vec![b("DEL"), b("a")]), RespValue::int(1));
    assert_eq!(rt.exec(vec![b("GET"), b("a")]), RespValue::null_bulk());
}

#[test]
fn shard_runtime_exec_mget_across_shards() {
    let rt = rt(4);
    for i in 0..20 {
        let resp = rt.exec(vec![
            b("SET"),
            Bytes::from(format!("key{i}")),
            Bytes::from(format!("v{i}")),
        ]);
        assert_eq!(resp, RespValue::ok());
    }
    let mut args = vec![b("MGET")];
    args.extend((0..20).map(|i| Bytes::from(format!("key{i}"))));
    let r = rt.exec(args);
    match r {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 20);
            for (i, item) in items.iter().enumerate() {
                assert_eq!(*item, RespValue::bulk(Bytes::from(format!("v{i}"))));
            }
        }
        other => panic!("bad mget via shard runtime: {other:?}"),
    }
}

#[test]
fn shard_runtime_queue_full_returns_error() {
    let rt = Arc::new(spawn_shard_runtime(1, 1, system_now_ms));
    // Block the single worker inside a broadcast closure (production API).
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let rt_b = rt.clone();
    std::thread::spawn(move || {
        rt_b.broadcast(move |_, _| {
            release_rx.lock().unwrap().recv().ok();
        });
    });
    std::thread::sleep(Duration::from_millis(50));
    // Fill the capacity-1 queue behind the blocked worker.
    let (tx2, _rx2) = std::sync::mpsc::channel();
    rt.shards[0]
        .tx
        .try_send(Cmd::Exec {
            args: vec![b("SET"), b("k2"), b("v")],
            reply: tx2,
            aof: false,
        })
        .expect("queue should have room for one behind blocked worker");
    let resp = rt.exec(vec![b("SET"), b("k3"), b("v")]);
    assert!(
        matches!(&resp, RespValue::Error(m) if m.contains("shard command queue full")),
        "got {resp:?}"
    );
    release_tx.send(()).ok();
}

/// Block `sid`'s worker inside a Broadcast closure, then fill its capacity-1
/// queue. Returns a release sender; caller must drop/send it after asserting.
fn block_shard_and_fill_queue(rt: &ShardRuntime, sid: usize) -> std::sync::mpsc::Sender<()> {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let (ack_tx, _ack_rx) = std::sync::mpsc::channel();
    rt.shards[sid]
        .tx
        .send(Cmd::Broadcast {
            f: Arc::new(move |_, _| {
                release_rx.lock().unwrap().recv().ok();
            }),
            reply: ack_tx,
        })
        .expect("idle shard accepts broadcast");
    std::thread::sleep(Duration::from_millis(50));
    let (fill_tx, _fill_rx) = std::sync::mpsc::channel();
    rt.shards[sid]
        .tx
        .try_send(Cmd::DelKey {
            key: b("filler"),
            now: 0,
            reply: fill_tx,
        })
        .expect("queue should have room for one behind blocked worker");
    release_tx
}

#[test]
fn del_queue_full_returns_error() {
    let rt = Arc::new(spawn_shard_runtime(1, 1, system_now_ms));
    let release_tx = block_shard_and_fill_queue(&rt, 0);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let rt_d = rt.clone();
    std::thread::spawn(move || {
        done_tx.send(rt_d.exec(vec![b("DEL"), b("k3")])).ok();
    });
    let resp = done_rx
        .recv_timeout(Duration::from_millis(300))
        .expect("DEL must not block on a full queue");
    assert!(
        matches!(&resp, RespValue::Error(m) if m.contains("shard command queue full")),
        "got {resp:?}"
    );
    release_tx.send(()).ok();
}

#[test]
fn rename_cross_shard_queue_full_returns_error() {
    let rt = Arc::new(spawn_shard_runtime(2, 1, system_now_ms));
    let mut src = None;
    let mut dst = None;
    for i in 0..64u32 {
        let k = Bytes::from(format!("rk{i}"));
        let sid = rt.shard_id_for_key(&k);
        if src.is_none() {
            src = Some((k, sid));
        } else if Some(sid) != src.as_ref().map(|(_, s)| *s) {
            dst = Some(k);
            break;
        }
    }
    let (src, sid_s) = src.expect("need a src key");
    let dst = dst.expect("need a dst key on another shard");
    // Block the src shard (FetchRemove is the first cross-shard step).
    let release_tx = block_shard_and_fill_queue(&rt, sid_s);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let rt_r = rt.clone();
    std::thread::spawn(move || {
        done_tx.send(rt_r.exec(vec![b("RENAME"), src, dst])).ok();
    });
    let resp = done_rx
        .recv_timeout(Duration::from_millis(300))
        .expect("RENAME must not block on a full queue");
    assert!(
        matches!(&resp, RespValue::Error(m) if m.contains("shard command queue full")),
        "got {resp:?}"
    );
    release_tx.send(()).ok();
}

#[test]
fn del_actor_crashed_returns_error() {
    let rt = Arc::new(spawn_shard_runtime(1, 8, system_now_ms));
    let rt_b = rt.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        rt_b.broadcast(|_, _| panic!("simulated actor crash"));
        done_tx.send(()).ok();
    });
    done_rx
        .recv_timeout(Duration::from_millis(1000))
        .expect("broadcast returns once the worker dies");
    let resp = rt.exec(vec![b("DEL"), b("k")]);
    assert!(
        matches!(&resp, RespValue::Error(m) if m.contains("shard actor crashed")),
        "got {resp:?}"
    );
}

#[test]
fn config_set_surfaces_actor_crash() {
    let rt = Arc::new(spawn_shard_runtime(1, 8, system_now_ms));
    let rt_b = rt.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        rt_b.broadcast(|_, _| panic!("simulated actor crash"));
        done_tx.send(()).ok();
    });
    done_rx
        .recv_timeout(Duration::from_millis(1000))
        .expect("broadcast returns once the worker dies");
    let resp = rt.exec(vec![b("CONFIG"), b("SET"), b("maxmemory"), b("1000")]);
    assert!(
        matches!(&resp, RespValue::Error(m) if m.contains("shard actor crashed")),
        "CONFIG SET must surface actor crash, got {resp:?}"
    );
}

// --- Multi-shard coordinator-lite routing ---

#[test]
fn multi_shard_mget_consistent_via_runtime() {
    let rt = rt(4);
    for i in 0..20 {
        rt.exec(vec![
            b("SET"),
            Bytes::from(format!("key{i}")),
            Bytes::from(format!("v{i}")),
        ]);
    }
    let mut args = vec![b("MGET")];
    args.extend((0..20).map(|i| Bytes::from(format!("key{i}"))));
    let r = rt.exec(args);
    match r {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 20);
            for (i, item) in items.iter().enumerate() {
                assert_eq!(*item, RespValue::bulk(Bytes::from(format!("v{i}"))));
            }
        }
        other => panic!("bad mget: {other:?}"),
    }
}

#[test]
fn multi_shard_flushall_and_dbsize() {
    let rt = rt(3);
    rt.exec(vec![b("SET"), b("x"), b("1")]);
    rt.exec(vec![b("SET"), b("y"), b("2")]);
    assert_eq!(rt.exec(vec![b("DBSIZE")]), RespValue::int(2));
    assert_eq!(rt.exec(vec![b("FLUSHALL")]), RespValue::ok());
    assert_eq!(rt.exec(vec![b("DBSIZE")]), RespValue::int(0));
    assert_eq!(rt.exec(vec![b("EXISTS"), b("x")]), RespValue::int(0));
}

#[test]
fn multi_shard_del_and_exists() {
    let rt = rt(4);
    for k in ["a", "b", "c", "d", "e"] {
        rt.exec(vec![b("SET"), b(k), b("v")]);
    }
    assert_eq!(
        rt.exec(vec![b("EXISTS"), b("a"), b("c"), b("e")]),
        RespValue::int(3)
    );
    assert_eq!(
        rt.exec(vec![b("DEL"), b("a"), b("c"), b("missing")]),
        RespValue::int(2)
    );
    assert_eq!(
        rt.exec(vec![b("EXISTS"), b("a"), b("c")]),
        RespValue::int(0)
    );
}

#[test]
fn multi_shard_keys_glob() {
    let rt = rt(2);
    for k in ["user:1", "user:2", "session:a", "other"] {
        rt.exec(vec![b("SET"), b(k), b("v")]);
    }
    let keys = rt.exec(vec![b("KEYS"), b("user:*")]);
    match keys {
        RespValue::Array(Some(items)) => {
            let mut names: Vec<String> = items
                .iter()
                .map(|x| match x {
                    RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                    other => panic!("bad item {other:?}"),
                })
                .collect();
            names.sort();
            assert_eq!(names, vec!["user:1".to_string(), "user:2".to_string()]);
        }
        other => panic!("bad keys {other:?}"),
    }
}

#[test]
fn multi_shard_info_aggregates() {
    let rt = rt(2);
    rt.exec(vec![b("SET"), b("k1"), b("v1")]);
    rt.exec(vec![b("SET"), b("k2"), b("v2")]);
    match rt.exec(vec![b("INFO")]) {
        RespValue::BulkString(Some(t)) => {
            let text = String::from_utf8_lossy(&t);
            assert!(text.contains("MemoryDataBase_version"), "info: {text}");
            assert!(text.contains("db0:keys=2"), "info keys: {text}");
        }
        other => panic!("bad info {other:?}"),
    }
}

#[test]
fn multi_shard_rename_cross_shard() {
    let rt = rt(4);
    let mut src = None;
    let mut dst = None;
    for i in 0..64u32 {
        let k = Bytes::from(format!("rk{i}"));
        let sid = rt.shard_id_for_key(&k);
        if src.is_none() {
            src = Some((k, sid));
        } else if Some(sid) != src.as_ref().map(|(_, s)| *s) {
            dst = Some(k);
            break;
        }
    }
    let (src, _) = src.expect("need a src key");
    let dst = dst.expect("need a dst key on another shard");
    rt.exec(vec![b("SET"), src.clone(), b("v")]);
    assert_eq!(
        rt.exec(vec![b("RENAME"), src.clone(), dst.clone()]),
        RespValue::ok()
    );
    assert_eq!(rt.exec(vec![b("EXISTS"), src.clone()]), RespValue::int(0));
    assert_eq!(
        rt.exec(vec![b("GET"), dst.clone()]),
        RespValue::bulk(b("v"))
    );
}

#[test]
fn multi_shard_rename_same_shard_and_missing() {
    let rt = rt(2);
    rt.exec(vec![b("SET"), b("s"), b("1")]);
    assert_eq!(rt.exec(vec![b("RENAME"), b("s"), b("s")]), RespValue::ok());
    assert_eq!(
        rt.exec(vec![b("RENAME"), b("nope"), b("n2")]),
        RespValue::err("ERR no such key")
    );
}

#[test]
fn config_set_get_via_runtime() {
    let rt = rt(2);
    assert_eq!(
        rt.exec(vec![b("CONFIG"), b("SET"), b("maxmemory"), b("1048576")]),
        RespValue::ok()
    );
    assert_eq!(rt.maxmemory(), Some(1048576));
    match rt.exec(vec![b("CONFIG"), b("GET"), b("maxmemory")]) {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[0], RespValue::bulk(b("maxmemory")));
            assert_eq!(items[1], RespValue::bulk(b("1048576")));
        }
        other => panic!("bad config get {other:?}"),
    }
    assert_eq!(
        rt.exec(vec![
            b("CONFIG"),
            b("SET"),
            b("maxmemory-policy"),
            b("allkeys-lru")
        ]),
        RespValue::ok()
    );
    assert_eq!(rt.maxmemory_policy(), MaxMemoryPolicy::AllKeysLru);
    match rt.exec(vec![b("CONFIG"), b("GET"), b("maxmemory-policy")]) {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[1], RespValue::bulk(b("allkeys-lru")));
        }
        other => panic!("bad policy get {other:?}"),
    }
}

#[test]
fn broadcast_affects_all_shards() {
    let rt = Arc::new(rt(3));
    for i in 0..3 {
        rt.exec(vec![b("SET"), Bytes::from(format!("k{i}")), b("v")]);
    }
    assert_eq!(rt.exec(vec![b("DBSIZE")]), RespValue::int(3));
    rt.broadcast(|_, sh| {
        sh.dict.clear();
        sh.mem = 0;
    });
    assert_eq!(rt.exec(vec![b("DBSIZE")]), RespValue::int(0));
}

#[test]
fn snapshot_and_load_shard_roundtrip() {
    let e = rt(2);
    e.exec(vec![b("SET"), b("a"), b("1")]);
    e.exec(vec![b("SET"), b("b"), b("2")]);
    let snaps: Vec<_> = (0..e.shard_count())
        .map(|sid| e.snapshot_shard(sid))
        .collect();
    let total: usize = snaps.iter().map(|s| s.len()).sum();
    assert_eq!(total, 2);
    let e2 = rt(2);
    for (sid, entries) in snaps.into_iter().enumerate() {
        e2.load_shard(sid, entries);
    }
    assert_eq!(e2.exec(vec![b("GET"), b("a")]), RespValue::bulk(b("1")));
    assert!(e2.total_mem() > 0, "load_shard must account mem");
}

#[test]
fn expire_cycle_removes_expired() {
    let rt = rt(2);
    rt.exec(vec![b("SET"), b("gone"), b("v")]);
    rt.exec(vec![b("EXPIRE"), b("gone"), b("0")]);
    rt.exec(vec![b("SET"), b("kept"), b("v2")]);
    std::thread::sleep(Duration::from_millis(20));
    let removed = rt.expire_cycle(system_now_ms(), 100);
    assert!(
        removed >= 1,
        "expire_cycle should remove expired key, removed={removed}"
    );
    assert_eq!(rt.exec(vec![b("GET"), b("kept")]), RespValue::bulk(b("v2")));
}

#[test]
fn mset_across_shards() {
    let rt = rt(4);
    let resp = rt.exec(vec![
        b("MSET"),
        b("k1"),
        b("v1"),
        b("k2"),
        b("v2"),
        b("k3"),
        b("v3"),
    ]);
    assert_eq!(resp, RespValue::ok());
    assert_eq!(rt.exec(vec![b("GET"), b("k1")]), RespValue::bulk(b("v1")));
    assert_eq!(rt.exec(vec![b("GET"), b("k3")]), RespValue::bulk(b("v3")));
}

#[test]
fn msetnx_across_shards() {
    let rt = rt(4);
    let resp = rt.exec(vec![b("MSETNX"), b("n1"), b("v1"), b("n2"), b("v2")]);
    assert_eq!(resp, RespValue::int(1));
    let resp = rt.exec(vec![b("MSETNX"), b("n1"), b("other"), b("n3"), b("v3")]);
    assert_eq!(resp, RespValue::int(0));
    assert_eq!(rt.exec(vec![b("GET"), b("n1")]), RespValue::bulk(b("v1")));
}

#[test]
fn save_family_rejected_by_runtime() {
    let rt = rt(1);
    for cmd in ["SAVE", "BGSAVE", "LASTSAVE"] {
        let resp = rt.exec(vec![b(cmd)]);
        let msg = match &resp {
            RespValue::Error(s) => s.clone(),
            other => panic!("{cmd}: expected Error, got {other:?}"),
        };
        assert!(msg.contains("unknown command"), "{cmd}: {msg}");
        assert!(msg.contains(cmd), "{cmd}: {msg}");
    }
}

#[test]
fn set_maxmemory_policy_only_does_not_oom() {
    let rt = rt(2);
    assert_eq!(
        rt.exec(vec![
            b("CONFIG"),
            b("SET"),
            b("maxmemory-policy"),
            b("allkeys-lru")
        ]),
        RespValue::ok()
    );
    assert_eq!(rt.maxmemory(), None);
    assert_eq!(rt.exec(vec![b("SET"), b("k"), b("v")]), RespValue::ok());
}

#[test]
fn randomkey_returns_a_key() {
    let rt = rt(2);
    rt.exec(vec![b("SET"), b("only"), b("v")]);
    match rt.exec(vec![b("RANDOMKEY")]) {
        RespValue::BulkString(Some(k)) => assert_eq!(k, b("only")),
        other => panic!("bad randomkey {other:?}"),
    }
    rt.exec(vec![b("FLUSHALL")]);
    assert_eq!(rt.exec(vec![b("RANDOMKEY")]), RespValue::null_bulk());
}

#[test]
fn shard_id_for_key_stable_and_in_range() {
    let rt = spawn_shard_runtime(4, 16, system_now_ms);
    for k in ["a", "b", "user:1", "x"] {
        let sid = rt.shard_id_for_key(&b(k));
        assert!(sid < 4);
        assert_eq!(sid, rt.shard_id_for_key(&b(k)));
    }
    assert_eq!(rt.shard_count(), 4);
}

// --- Task 4: actor-owned AOF append wiring ---

/// Records `(sid, args)` calls made by the shard actors.
struct RecordingSink {
    calls: Mutex<Vec<(usize, Vec<Bytes>)>>,
}

impl AofSink for RecordingSink {
    fn append(&self, sid: usize, args: &[Bytes]) {
        self.calls.lock().unwrap().push((sid, args.to_vec()));
    }
}

/// After a successful write command, the owning actor must call the AOF sink
/// with that shard's id and the original args. Reads and failed writes must
/// not call it. `exec_no_aof` must never call it.
#[test]
fn actor_appends_successful_writes_to_aof_sink() {
    let sink = Arc::new(RecordingSink {
        calls: Mutex::new(Vec::new()),
    });
    let rt = spawn_shard_runtime(2, 1024, system_now_ms);
    rt.set_aof_sink(Some(sink.clone()));

    assert_eq!(rt.exec(vec![b("SET"), b("a"), b("1")]), RespValue::ok());
    assert_eq!(rt.exec(vec![b("SET"), b("b"), b("2")]), RespValue::ok());
    // Read (Ok) — must not append.
    let _ = rt.exec(vec![b("GET"), b("a")]);
    // Failed write — must not append.
    let _ = rt.exec(vec![b("SET")]);
    // exec_no_aof — must not append even for a write.
    assert_eq!(
        rt.exec_no_aof(vec![b("SET"), b("c"), b("3")]),
        RespValue::ok()
    );

    let calls = sink.calls.lock().unwrap().clone();
    assert_eq!(
        calls.len(),
        2,
        "only the two successful SETs append: {calls:?}"
    );
    for (sid, args) in &calls {
        assert!(*sid < 2);
        assert_eq!(args[0], b("SET"));
    }
    // Each key was appended on its owning shard.
    let mut seen: Vec<&Bytes> = calls.iter().map(|(_, a)| &a[1]).collect();
    seen.sort();
    assert_eq!(seen, vec![&b("a"), &b("b")]);
}

/// Removing the sink must stop appends (CONFIG appendonly is static for MVP,
/// but recovery detaches/attaches sinks at server level).
#[test]
fn actor_stops_appending_after_sink_removed() {
    let sink = Arc::new(RecordingSink {
        calls: Mutex::new(Vec::new()),
    });
    let rt = spawn_shard_runtime(1, 1024, system_now_ms);
    rt.set_aof_sink(Some(sink.clone()));
    rt.exec(vec![b("SET"), b("k"), b("v")]);
    rt.set_aof_sink(None);
    rt.exec(vec![b("SET"), b("k2"), b("v2")]);
    assert_eq!(
        sink.calls.lock().unwrap().len(),
        1,
        "no append after sink removal"
    );
}

// --- Coordinator paths: DEL/FLUSHALL/RENAME/MSET must reach the AOF sink ---

fn sink_calls(sink: &RecordingSink) -> Vec<(usize, Vec<Bytes>)> {
    sink.calls.lock().unwrap().clone()
}

fn find_cross_shard_pair(rt: &ShardRuntime) -> (Bytes, Bytes) {
    let mut src = None;
    let mut dst = None;
    for i in 0..64u32 {
        let k = Bytes::from(format!("rk{i}"));
        let sid = rt.shard_id_for_key(&k);
        if src.is_none() {
            src = Some((k, sid));
        } else if Some(sid) != src.as_ref().map(|(_, s)| *s) {
            dst = Some(k);
            break;
        }
    }
    let (src, _) = src.expect("need a src key");
    let dst = dst.expect("need a dst key on another shard");
    (src, dst)
}

/// DEL routes through Cmd::DelKey (not Cmd::Exec) but must still append
/// `DEL <keys...>` to each affected shard's AOF sink after successful deletes.
#[test]
fn coordinator_del_appends_to_aof_sink() {
    let sink = Arc::new(RecordingSink {
        calls: Mutex::new(Vec::new()),
    });
    let rt = spawn_shard_runtime(2, 1024, system_now_ms);
    rt.set_aof_sink(Some(sink.clone()));
    // Pick two keys that land on different shards.
    let mut k1 = None;
    let mut k2 = None;
    for i in 0..64u32 {
        let k = Bytes::from(format!("dk{i}"));
        let sid = rt.shard_id_for_key(&k);
        if k1.is_none() {
            k1 = Some((k, sid));
        } else if Some(sid) != k1.as_ref().map(|(_, s)| *s) {
            k2 = Some(k);
            break;
        }
    }
    let (k1, sid1) = k1.expect("need k1");
    let k2 = k2.expect("need k2 on another shard");
    let sid2 = rt.shard_id_for_key(&k2);
    assert_eq!(rt.exec(vec![b("SET"), k1.clone(), b("1")]), RespValue::ok());
    assert_eq!(rt.exec(vec![b("SET"), k2.clone(), b("2")]), RespValue::ok());
    // Clear SET noise; we only assert on DEL records.
    sink.calls.lock().unwrap().clear();

    assert_eq!(
        rt.exec(vec![b("DEL"), k1.clone(), k2.clone()]),
        RespValue::int(2)
    );
    let calls = sink_calls(&sink);
    let dels: Vec<(usize, Vec<Bytes>)> = calls
        .into_iter()
        .filter(|(_, a)| a.first().map(|n| n.as_ref() == b"DEL").unwrap_or(false))
        .collect();
    assert_eq!(
        dels.len(),
        2,
        "DEL must append on each affected shard: {dels:?}"
    );
    for (sid, args) in &dels {
        assert_eq!(args[0].as_ref(), b"DEL");
        assert_eq!(args.len(), 2, "batched per-shard DEL has one key here");
        let key = &args[1];
        let expect_sid = if *key == k1 { sid1 } else { sid2 };
        assert_eq!(*sid, expect_sid, "DEL must land on the key's owning shard");
    }
    // exec_no_aof must not append DEL.
    sink.calls.lock().unwrap().clear();
    rt.exec(vec![b("SET"), b("c"), b("3")]);
    sink.calls.lock().unwrap().clear();
    assert_eq!(rt.exec_no_aof(vec![b("DEL"), b("c")]), RespValue::int(1));
    assert!(
        sink_calls(&sink).is_empty(),
        "exec_no_aof DEL must not append"
    );
}

/// FLUSHALL must append the command to every shard sink **before** clearing.
#[test]
fn coordinator_flushall_appends_to_all_sinks() {
    let sink = Arc::new(RecordingSink {
        calls: Mutex::new(Vec::new()),
    });
    let rt = spawn_shard_runtime(3, 1024, system_now_ms);
    rt.set_aof_sink(Some(sink.clone()));
    rt.exec(vec![b("SET"), b("x"), b("1")]);
    sink.calls.lock().unwrap().clear();

    assert_eq!(rt.exec(vec![b("FLUSHALL")]), RespValue::ok());
    let calls = sink_calls(&sink);
    let flushes: Vec<&(usize, Vec<Bytes>)> = calls
        .iter()
        .filter(|(_, a)| {
            a.first()
                .map(|n| n.as_ref() == b"FLUSHALL")
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(
        flushes.len(),
        3,
        "FLUSHALL must append on all 3 shards: {calls:?}"
    );
    let mut sids: Vec<usize> = flushes.iter().map(|(s, _)| *s).collect();
    sids.sort();
    assert_eq!(sids, vec![0, 1, 2]);

    sink.calls.lock().unwrap().clear();
    rt.exec_no_aof(vec![b("FLUSHDB")]);
    assert!(
        sink_calls(&sink).iter().all(|(_, a)| a
            .first()
            .map(|n| n.as_ref() != b"FLUSHALL" && n.as_ref() != b"FLUSHDB")
            .unwrap_or(true)),
        "exec_no_aof FLUSHALL/FLUSHDB must not append"
    );
}

/// Cross-shard RENAME must log `DEL src` on the source shard and, on the
/// destination shard, `DEL dst` **before** the value command so recovery
/// cannot replay the value on top of a pre-existing dst container.
#[test]
fn coordinator_rename_cross_shard_appends_to_aof_sink() {
    let sink = Arc::new(RecordingSink {
        calls: Mutex::new(Vec::new()),
    });
    let rt = spawn_shard_runtime(2, 1024, system_now_ms);
    rt.set_aof_sink(Some(sink.clone()));
    let (src, dst) = find_cross_shard_pair(&rt);
    let sid_s = rt.shard_id_for_key(&src);
    let sid_d = rt.shard_id_for_key(&dst);
    // Pre-existing dst container (live-only) + src list, so the value command
    // on sid_d is RPUSH (the case where a missing DEL dst corrupts recovery).
    rt.exec_no_aof(vec![b("RPUSH"), dst.clone(), b("old")]);
    assert_eq!(
        rt.exec(vec![b("RPUSH"), src.clone(), b("v")]),
        RespValue::Integer(1)
    );
    sink.calls.lock().unwrap().clear();

    assert_eq!(
        rt.exec(vec![b("RENAME"), src.clone(), dst.clone()]),
        RespValue::ok()
    );
    assert_eq!(rt.exec(vec![b("EXISTS"), src.clone()]), RespValue::int(0));
    assert_eq!(
        rt.exec(vec![b("LRANGE"), dst.clone(), b("0"), b("-1")]),
        RespValue::Array(Some(vec![RespValue::bulk(b("v"))])),
        "live rename must replace dst, not append"
    );

    let calls = sink_calls(&sink);
    let del_src = calls.iter().find(|(sid, a)| {
        *sid == sid_s && a.first().map(|n| n.as_ref() == b"DEL").unwrap_or(false) && a[1] == src
    });
    assert!(
        del_src.is_some(),
        "cross-shard RENAME must append DEL src to {sid_s}: {calls:?}"
    );
    let dst_recs: Vec<&(usize, Vec<Bytes>)> =
        calls.iter().filter(|(sid, _)| *sid == sid_d).collect();
    let pos = |pred: &dyn Fn(&Vec<Bytes>) -> bool| dst_recs.iter().position(|(_, a)| pred(a));
    let del_dst = pos(&|a| {
        a.first().map(|n| n.as_ref() == b"DEL").unwrap_or(false) && a.len() == 2 && a[1] == dst
    })
    .unwrap_or_else(|| panic!("cross-shard RENAME must append DEL dst to {sid_d}: {calls:?}"));
    let rpush_dst = pos(&|a| {
        a.first().map(|n| n.as_ref() == b"RPUSH").unwrap_or(false)
            && a[1] == dst
            && a.get(2).map(|v| v.as_ref() == b"v").unwrap_or(false)
    })
    .unwrap_or_else(|| panic!("cross-shard RENAME must append RPUSH dst to {sid_d}: {calls:?}"));
    assert!(
        del_dst < rpush_dst,
        "DEL dst must precede the value command on {sid_d}: {calls:?}"
    );

    // exec_no_aof must not append on the cross-shard path either.
    sink.calls.lock().unwrap().clear();
    let (src2, dst2) = find_cross_shard_pair(&rt);
    // Re-use a fresh key pair: SET without sink noise then exec_no_aof RENAME.
    // (src2/dst2 are distinct keys from src/dst.)
    rt.exec_no_aof(vec![b("SET"), src2.clone(), b("w")]);
    sink.calls.lock().unwrap().clear();
    assert_eq!(
        rt.exec_no_aof(vec![b("RENAME"), src2.clone(), dst2.clone()]),
        RespValue::ok()
    );
    assert!(
        sink_calls(&sink).is_empty(),
        "exec_no_aof cross-shard RENAME must not append"
    );
}

/// MSET must buffer per-shard subsets and append only after every shard
/// succeeded (partial-failure AOF gap closed).
#[test]
fn coordinator_mset_appends_subsets_after_all_shards_succeed() {
    let sink = Arc::new(RecordingSink {
        calls: Mutex::new(Vec::new()),
    });
    let rt = spawn_shard_runtime(2, 1024, system_now_ms);
    rt.set_aof_sink(Some(sink.clone()));

    assert_eq!(
        rt.exec(vec![
            b("MSET"),
            b("m1"),
            b("v1"),
            b("m2"),
            b("v2"),
            b("m3"),
            b("v3")
        ]),
        RespValue::ok()
    );
    let calls = sink_calls(&sink);
    let msets: Vec<&(usize, Vec<Bytes>)> = calls
        .iter()
        .filter(|(_, a)| a.first().map(|n| n.as_ref() == b"MSET").unwrap_or(false))
        .collect();
    assert!(
        !msets.is_empty(),
        "successful MSET must append subsets: {calls:?}"
    );
    // Every key appears in exactly one subset, on its owning shard.
    let mut seen = Vec::new();
    for (sid, args) in &msets {
        assert_eq!(args[0].as_ref(), b"MSET");
        assert_eq!(
            (args.len() - 1) % 2,
            0,
            "MSET subset must be key/value pairs"
        );
        for pair in args[1..].chunks(2) {
            assert_eq!(*sid, rt.shard_id_for_key(&pair[0]));
            seen.push(pair[0].clone());
        }
    }
    seen.sort();
    assert_eq!(seen, vec![b("m1"), b("m2"), b("m3")]);

    // exec_no_aof MSET must not append.
    sink.calls.lock().unwrap().clear();
    assert_eq!(
        rt.exec_no_aof(vec![b("MSET"), b("n1"), b("x")]),
        RespValue::ok()
    );
    assert!(
        sink_calls(&sink)
            .iter()
            .all(|(_, a)| a.first().map(|n| n.as_ref() != b"MSET").unwrap_or(true)),
        "exec_no_aof MSET must not append"
    );
}
