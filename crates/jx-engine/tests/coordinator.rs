use bytes::Bytes;
use jx_engine::actor::{spawn_shard_runtime, Cmd, ShardRuntime};
use jx_engine::{coordinator, system_now_ms};
use jx_proto::RespValue;
use std::sync::Arc;
use std::time::Duration;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn rt4() -> ShardRuntime {
    spawn_shard_runtime(4, 256, system_now_ms)
}

#[test]
fn mget_preserves_input_order_across_shards() {
    let rt = rt4();
    for i in 0..20 {
        rt.exec(vec![
            b("SET"),
            Bytes::from(format!("k{i}")),
            Bytes::from(format!("v{i}")),
        ]);
    }
    let mut args = vec![b("MGET")];
    for i in 0..20 {
        args.push(Bytes::from(format!("k{i}")));
    }
    match coordinator::mget_coalesced(&rt, &args[1..]) {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 20);
            for (i, it) in items.iter().enumerate() {
                assert_eq!(it, &RespValue::bulk(Bytes::from(format!("v{i}"))));
            }
        }
        other => panic!("bad {other:?}"),
    }
}

#[test]
fn mget_missing_keys_are_null_bulk() {
    let rt = rt4();
    rt.exec(vec![b("SET"), b("a"), b("1")]);
    rt.exec(vec![b("SET"), b("b"), b("2")]);
    match coordinator::mget_coalesced(&rt, &[b("a"), b("missing"), b("b")]) {
        RespValue::Array(Some(items)) => {
            assert_eq!(
                items,
                vec![
                    RespValue::bulk(b("1")),
                    RespValue::null_bulk(),
                    RespValue::bulk(b("2"))
                ]
            );
        }
        other => panic!("bad {other:?}"),
    }
}

#[test]
fn mget_empty_keys_returns_empty_array() {
    let rt = rt4();
    assert_eq!(
        coordinator::mget_coalesced(&rt, &[]),
        RespValue::array(vec![])
    );
}

#[test]
fn mget_queue_full_returns_error() {
    // capacity-1 runtime; block shard 0, fill its queue, then MGET a key
    // that hashes to shard 0 → fan-out try_send must surface queue-full.
    let rt = Arc::new(spawn_shard_runtime(1, 1, system_now_ms));
    let key_on_shard0 = {
        let mut k = None;
        for i in 0..256u32 {
            let cand = Bytes::from(format!("mk{i}"));
            if rt.shard_id_for_key(&cand) == 0 {
                k = Some(cand);
                break;
            }
        }
        k.expect("some key maps to shard 0")
    };
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let (ack_tx, _ack_rx) = std::sync::mpsc::channel();
    rt.shards[0]
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
    rt.shards[0]
        .tx
        .try_send(Cmd::DelKey {
            key: b("filler"),
            now: 0,
            reply: fill_tx,
        })
        .expect("queue has room for one behind blocked worker");

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let rt_m = rt.clone();
    let k2 = key_on_shard0.clone();
    std::thread::spawn(move || {
        done_tx.send(coordinator::mget_coalesced(&rt_m, &[k2])).ok();
    });
    let resp = done_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("MGET fan-out must not block on a full queue");
    assert!(
        matches!(&resp, RespValue::Error(m) if m.contains("shard command queue full")),
        "got {resp:?}"
    );
    release_tx.send(()).ok();
}

#[test]
fn keys_glob_collects_across_shards() {
    let rt = rt4();
    for k in ["user:1", "user:2", "session:a", "other"] {
        rt.exec(vec![b("SET"), b(k), b("v")]);
    }
    match coordinator::keys_glob(&rt, &b("user:*")) {
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
fn flushall_clears_all_shards() {
    let rt = rt4();
    rt.exec(vec![b("SET"), b("x"), b("1")]);
    rt.exec(vec![b("SET"), b("y"), b("2")]);
    assert_eq!(coordinator::flushall(&rt), RespValue::ok());
    assert_eq!(coordinator::dbsize(&rt), RespValue::int(0));
    assert_eq!(rt.exec(vec![b("EXISTS"), b("x")]), RespValue::int(0));
    assert_eq!(rt.exec(vec![b("EXISTS"), b("y")]), RespValue::int(0));
}

#[test]
fn dbsize_sums_across_shards() {
    let rt = rt4();
    assert_eq!(coordinator::dbsize(&rt), RespValue::int(0));
    for k in ["a", "b", "c", "d", "e"] {
        rt.exec(vec![b("SET"), b(k), b("v")]);
    }
    assert_eq!(coordinator::dbsize(&rt), RespValue::int(5));
    rt.exec(vec![b("DEL"), b("a"), b("b")]);
    assert_eq!(coordinator::dbsize(&rt), RespValue::int(3));
}

#[test]
fn info_aggregates_across_shards() {
    let rt = rt4();
    rt.exec(vec![b("SET"), b("k1"), b("v1")]);
    rt.exec(vec![b("SET"), b("k2"), b("v2")]);
    match coordinator::info(&rt) {
        RespValue::BulkString(Some(t)) => {
            let text = String::from_utf8_lossy(&t);
            assert!(text.contains("MemoryDataBase_version"), "info: {text}");
            assert!(text.contains("db0:keys=2"), "info keys: {text}");
        }
        other => panic!("bad info {other:?}"),
    }
}

#[test]
fn exec_delegates_to_coordinator() {
    let rt = rt4();
    for i in 0..10 {
        rt.exec(vec![
            b("SET"),
            Bytes::from(format!("e{i}")),
            Bytes::from(format!("ev{i}")),
        ]);
    }
    let mut args = vec![b("MGET")];
    for i in 0..10 {
        args.push(Bytes::from(format!("e{i}")));
    }
    let via_exec = rt.exec(args.clone());
    let via_coord = coordinator::mget_coalesced(&rt, &args[1..]);
    assert_eq!(via_exec, via_coord, "MGET: exec must equal coordinator API");

    assert_eq!(rt.exec(vec![b("DBSIZE")]), coordinator::dbsize(&rt));
    assert_eq!(rt.exec(vec![b("FLUSHALL")]), coordinator::flushall(&rt));

    rt.exec(vec![b("SET"), b("user:1"), b("v")]);
    rt.exec(vec![b("SET"), b("user:2"), b("v")]);
    let via_exec = rt.exec(vec![b("KEYS"), b("user:*")]);
    let via_coord = coordinator::keys_glob(&rt, &b("user:*"));
    assert_eq!(via_exec, via_coord, "KEYS: exec must equal coordinator API");
}
