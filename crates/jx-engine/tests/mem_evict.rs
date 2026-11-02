use bytes::Bytes;
use jx_engine::actor::{spawn_shard_runtime, ShardRuntime};
use jx_engine::{system_now_ms, MaxMemoryPolicy};
use jx_proto::RespValue;

fn eng() -> ShardRuntime {
    spawn_shard_runtime(1, 1024, system_now_ms)
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn del_all(e: &ShardRuntime, keys: &[Bytes]) {
    let mut args = vec![b("DEL")];
    args.extend_from_slice(keys);
    e.exec(args);
}

#[test]
fn set_many_then_del_mem_decreases() {
    let e = eng();
    let keys: Vec<Bytes> = (0..10).map(|i| b(&format!("key{i}"))).collect();
    for k in &keys {
        e.exec(vec![b("SET"), k.clone(), b("some-value-payload")]);
    }
    let peak = e.total_mem();
    assert!(peak > 0, "peak mem should be positive after 10 SETs");
    del_all(&e, &keys);
    let after = e.total_mem();
    assert!(
        after < peak,
        "mem after DEL all ({after}) must be strictly less than peak ({peak})"
    );
}

#[test]
fn noeviction_oom_then_del_allows_write() {
    let e = eng();
    e.set_maxmemory(200, MaxMemoryPolicy::NoEviction)
        .expect("set_maxmemory on live shards");
    let mut inserted: Vec<Bytes> = Vec::new();
    let mut oom = false;
    while !oom {
        assert!(inserted.len() < 1000, "should OOM before 1000 keys");
        let key = b(&format!("k{}", inserted.len()));
        match e.exec(vec![b("SET"), key.clone(), b("xxxxxxxxxxxxxxxx")]) {
            RespValue::Error(err) => {
                assert!(err.contains("OOM"), "expected OOM error, got {err:?}");
                oom = true;
            }
            _ => inserted.push(key),
        }
    }
    assert!(
        !inserted.is_empty(),
        "should have inserted some keys before OOM"
    );
    del_all(&e, &inserted);
    assert_eq!(
        e.exec(vec![b("SET"), b("after"), b("v")]),
        RespValue::ok(),
        "SET must succeed again after DEL all under NoEviction"
    );
}

#[test]
fn incr_evicts_or_ooms_under_maxmemory() {
    let e = eng();
    let limit = 250usize;
    e.set_maxmemory(limit, MaxMemoryPolicy::AllKeysLru)
        .expect("set_maxmemory on live shards");
    let mut errors = 0usize;
    let mut successes = 0usize;
    for i in 0..50 {
        let key = b(&format!("n{i}"));
        match e.exec(vec![b("INCR"), key.clone()]) {
            RespValue::Error(err) => {
                assert!(err.contains("OOM"), "expected OOM error, got {err:?}");
                errors += 1;
            }
            _ => successes += 1,
        }
        assert!(
            e.total_mem() <= limit.saturating_mul(4),
            "mem {} must not grow unbounded under maxmemory {limit} (errors={errors}, successes={successes})",
            e.total_mem()
        );
    }
    assert!(successes > 0, "some INCR calls should succeed");
    let _ = errors;
}

#[test]
fn evict_skips_incoming_key() {
    let e = eng();
    e.set_maxmemory(200, MaxMemoryPolicy::AllKeysLru)
        .expect("set_maxmemory on live shards");
    let big = Bytes::from(vec![b'x'; 30]);
    // Fill well past maxmemory so eviction pressure is real.
    for i in 0..6 {
        let key = b(&format!("f{i}"));
        e.exec(vec![b("SET"), key.clone(), big.clone()]);
    }
    assert!(e.total_mem() >= 200, "should be under memory pressure");
    // Write "existing" — its own evict_if_needed must skip it as a victim.
    e.exec(vec![b("SET"), b("existing"), big.clone()]);
    assert_eq!(e.exec(vec![b("EXISTS"), b("existing")]), RespValue::int(1));
    // Immediately SETNX the same key under pressure: must return 0 (exists) or OOM,
    // never 1. Eviction must not remove the incoming key.
    match e.exec(vec![b("SETNX"), b("existing"), big.clone()]) {
        RespValue::Integer(0) => {
            assert_eq!(e.exec(vec![b("EXISTS"), b("existing")]), RespValue::int(1));
        }
        RespValue::Error(err) => {
            assert!(err.contains("OOM"), "expected 0 or OOM, got {err:?}");
            // OOM means no non-incoming candidates; the key must still exist.
            assert_eq!(e.exec(vec![b("EXISTS"), b("existing")]), RespValue::int(1));
        }
        other => panic!("expected 0 or OOM, got {other:?}"),
    }
}

#[test]
fn config_set_maxmemory_zero_is_unlimited() {
    let e = eng();
    let r = e.exec(vec![b("CONFIG"), b("SET"), b("maxmemory"), b("0")]);
    assert_eq!(r, RespValue::ok());
    // Redis convention: GET of unset/unlimited maxmemory is "0".
    let r = e.exec(vec![b("CONFIG"), b("GET"), b("maxmemory")]);
    match r {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[1], RespValue::bulk(b("0")));
        }
        other => panic!("bad config get {other:?}"),
    }
    // SET must work (no OOM) after maxmemory=0.
    assert_eq!(e.exec(vec![b("SET"), b("k"), b("v")]), RespValue::ok());
    // Even if a shard is forced to Some(0) directly, evict treats it as unlimited.
    e.broadcast(|_, sh| {
        sh.maxmemory = Some(0);
    });
    assert_eq!(e.exec(vec![b("SET"), b("k2"), b("v")]), RespValue::ok());
}

#[test]
fn config_set_policy_only_with_unset_maxmemory_does_not_oom() {
    let e = eng();
    let r = e.exec(vec![
        b("CONFIG"),
        b("SET"),
        b("maxmemory-policy"),
        b("allkeys-lru"),
    ]);
    assert_eq!(r, RespValue::ok());
    assert_eq!(
        e.maxmemory(),
        None,
        "policy-only CONFIG SET must leave maxmemory unset (unlimited)"
    );
    assert_eq!(e.maxmemory_policy().as_str(), "allkeys-lru");
    // Subsequent SET must not OOM.
    assert_eq!(e.exec(vec![b("SET"), b("k"), b("v")]), RespValue::ok());
}

#[test]
fn config_set_maxmemory_tiny_still_ooms() {
    let e = eng();
    let r = e.exec(vec![b("CONFIG"), b("SET"), b("maxmemory"), b("100")]);
    assert_eq!(r, RespValue::ok());
    assert_eq!(e.maxmemory(), Some(100));
    let mut oom = false;
    for i in 0..1000 {
        if let RespValue::Error(err) =
            e.exec(vec![b("SET"), b(&format!("k{i}")), b("xxxxxxxxxxxxxxxx")])
        {
            assert!(err.contains("OOM"), "expected OOM error, got {err:?}");
            oom = true;
            break;
        }
    }
    assert!(oom, "maxmemory 100 must still OOM eventually");
}

fn assert_oom(resp: &RespValue) {
    match resp {
        RespValue::Error(m) => assert!(m.contains("OOM"), "expected OOM error, got {resp:?}"),
        other => panic!("expected OOM error, got {other:?}"),
    }
}

/// Grow a collection under small maxmemory + noeviction until OOM, then
/// shrink it and confirm writes are allowed again.
fn grow_oom_then_shrink_allows(
    e: &ShardRuntime,
    key: &Bytes,
    grow: impl Fn(&ShardRuntime, usize) -> RespValue,
    shrink: impl Fn(&ShardRuntime, usize) -> RespValue,
) {
    let mut oom = false;
    for i in 0..500 {
        if let resp @ RespValue::Error(_) = grow(e, i) {
            assert_oom(&resp);
            oom = true;
            break;
        }
    }
    assert!(
        oom,
        "collection growth must OOM under small maxmemory+noeviction"
    );
    // Shrink below the limit, then a fresh write on the same key must succeed.
    for i in 0..500 {
        let _ = shrink(e, i);
        match grow(e, 1000 + i) {
            resp @ RespValue::Error(_) => assert_oom(&resp),
            _ => return,
        }
    }
    panic!("shrinking {key:?} must allow writes again");
}

#[test]
fn hash_growth_ooms_then_hdel_allows_writes() {
    let e = eng();
    e.set_maxmemory(400, MaxMemoryPolicy::NoEviction)
        .expect("set_maxmemory on live shards");
    let key = b("h");
    let mem0 = e.total_mem();
    e.exec(vec![b("HSET"), key.clone(), b("seed"), b("1")]);
    let mem1 = e.total_mem();
    assert!(
        mem1 > mem0,
        "HSET must charge shard.mem (before={mem0}, after={mem1})"
    );
    grow_oom_then_shrink_allows(
        &e,
        &key,
        |e, i| {
            e.exec(vec![
                b("HSET"),
                key.clone(),
                Bytes::from(format!("f{i}")),
                b("xxxxxxxxxxxxxxxx"),
            ])
        },
        |e, i| e.exec(vec![b("HDEL"), key.clone(), Bytes::from(format!("f{i}"))]),
    );
}

#[test]
fn list_growth_ooms_then_pop_allows_writes() {
    let e = eng();
    e.set_maxmemory(400, MaxMemoryPolicy::NoEviction)
        .expect("set_maxmemory on live shards");
    let key = b("l");
    e.exec(vec![b("RPUSH"), key.clone(), b("seed")]);
    let mem0 = e.total_mem();
    e.exec(vec![b("RPUSH"), key.clone(), b("xxxxxxxxxxxxxxxx")]);
    assert!(e.total_mem() > mem0, "RPUSH must charge shard.mem");
    grow_oom_then_shrink_allows(
        &e,
        &key,
        |e, _| e.exec(vec![b("RPUSH"), key.clone(), b("xxxxxxxxxxxxxxxx")]),
        |e, _| e.exec(vec![b("RPOP"), key.clone()]),
    );
}

#[test]
fn set_growth_ooms_then_srem_allows_writes() {
    let e = eng();
    e.set_maxmemory(400, MaxMemoryPolicy::NoEviction)
        .expect("set_maxmemory on live shards");
    let key = b("s");
    e.exec(vec![b("SADD"), key.clone(), b("seed")]);
    let mem0 = e.total_mem();
    e.exec(vec![b("SADD"), key.clone(), b("xxxxxxxxxxxxxxxx")]);
    assert!(e.total_mem() > mem0, "SADD must charge shard.mem");
    grow_oom_then_shrink_allows(
        &e,
        &key,
        |e, i| e.exec(vec![b("SADD"), key.clone(), Bytes::from(format!("m{i}"))]),
        |e, i| e.exec(vec![b("SREM"), key.clone(), Bytes::from(format!("m{i}"))]),
    );
}

#[test]
fn zset_growth_ooms_then_zrem_allows_writes() {
    let e = eng();
    e.set_maxmemory(400, MaxMemoryPolicy::NoEviction)
        .expect("set_maxmemory on live shards");
    let key = b("z");
    e.exec(vec![b("ZADD"), key.clone(), b("1"), b("seed")]);
    let mem0 = e.total_mem();
    e.exec(vec![b("ZADD"), key.clone(), b("2"), b("xxxxxxxxxxxxxxxx")]);
    assert!(e.total_mem() > mem0, "ZADD must charge shard.mem");
    grow_oom_then_shrink_allows(
        &e,
        &key,
        |e, i| {
            e.exec(vec![
                b("ZADD"),
                key.clone(),
                Bytes::from(i.to_string()),
                Bytes::from(format!("m{i}")),
            ])
        },
        |e, i| e.exec(vec![b("ZREM"), key.clone(), Bytes::from(format!("m{i}"))]),
    );
}

#[test]
fn collection_mem_tracks_field_insert_and_remove() {
    let e = eng();
    let m0 = e.total_mem();
    e.exec(vec![b("HSET"), b("h"), b("f1"), b("v1")]);
    let m1 = e.total_mem();
    assert!(m1 > m0);
    e.exec(vec![b("HSET"), b("h"), b("f2"), b("v2")]);
    let m2 = e.total_mem();
    assert!(m2 > m1, "second HSET field must grow mem ({m1} -> {m2})");
    e.exec(vec![b("HDEL"), b("h"), b("f1"), b("f2")]);
    // Empty hash is dropped; mem returns to baseline.
    assert!(
        e.total_mem() <= m0 + 8,
        "empty hash must be dropped from mem (m0={m0}, now={})",
        e.total_mem()
    );
}
