use bytes::Bytes;
use jx_engine::actor::{spawn_shard_runtime, ShardRuntime};
use jx_engine::system_now_ms;
use jx_proto::RespValue;

fn eng() -> ShardRuntime {
    spawn_shard_runtime(1, 1024, system_now_ms)
}
fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

#[test]
fn hash_commands() {
    let e = eng();
    assert_eq!(
        e.exec(vec![b("HSET"), b("h"), b("f1"), b("v1"), b("f2"), b("v2")]),
        RespValue::int(2)
    );
    assert_eq!(
        e.exec(vec![b("HGET"), b("h"), b("f1")]),
        RespValue::bulk(b("v1"))
    );
    assert_eq!(e.exec(vec![b("HLEN"), b("h")]), RespValue::int(2));
    assert_eq!(
        e.exec(vec![b("HEXISTS"), b("h"), b("f2")]),
        RespValue::int(1)
    );
    assert!(matches!(
        e.exec(vec![b("HGETALL"), b("h")]),
        RespValue::Array(Some(_))
    ));
    assert_eq!(
        e.exec(vec![b("HINCRBY"), b("h"), b("n"), b("3")]),
        RespValue::int(3)
    );
    assert_eq!(e.exec(vec![b("HDEL"), b("h"), b("f1")]), RespValue::int(1));
}

#[test]
fn list_commands() {
    let e = eng();
    e.exec(vec![b("RPUSH"), b("l"), b("a"), b("b"), b("c")]);
    assert_eq!(e.exec(vec![b("LLEN"), b("l")]), RespValue::int(3));
    assert_eq!(
        e.exec(vec![b("LRANGE"), b("l"), b("0"), b("-1")]),
        RespValue::array(vec![
            RespValue::bulk(b("a")),
            RespValue::bulk(b("b")),
            RespValue::bulk(b("c"))
        ])
    );
    assert_eq!(e.exec(vec![b("LPOP"), b("l")]), RespValue::bulk(b("a")));
    assert_eq!(e.exec(vec![b("RPOP"), b("l")]), RespValue::bulk(b("c")));
    e.exec(vec![b("LPUSH"), b("l"), b("z")]);
    assert_eq!(
        e.exec(vec![b("LINDEX"), b("l"), b("0")]),
        RespValue::bulk(b("z"))
    );
}

#[test]
fn set_commands() {
    let e = eng();
    e.exec(vec![b("SADD"), b("s"), b("a"), b("b"), b("a")]);
    assert_eq!(e.exec(vec![b("SCARD"), b("s")]), RespValue::int(2));
    assert_eq!(
        e.exec(vec![b("SISMEMBER"), b("s"), b("a")]),
        RespValue::int(1)
    );
    assert_eq!(e.exec(vec![b("SREM"), b("s"), b("a")]), RespValue::int(1));
    assert_eq!(
        e.exec(vec![b("SMEMBERS"), b("s")]),
        RespValue::array(vec![RespValue::bulk(b("b"))])
    );
}

#[test]
fn srandmember_count_mode() {
    let e = eng();
    let members: &[&[u8]] = &[b"a", b"b", b"c", b"d", b"e"];
    e.exec(vec![
        b("SADD"),
        b("s"),
        b("a"),
        b("b"),
        b("c"),
        b("d"),
        b("e"),
    ]);
    for count in ["1", "2", "5"] {
        match e.exec(vec![b("SRANDMEMBER"), b("s"), b(count)]) {
            RespValue::Array(Some(items)) => {
                let want: usize = count.parse().unwrap();
                assert_eq!(items.len(), want, "count={count} len={}", items.len());
                for it in &items {
                    match it {
                        RespValue::BulkString(Some(v)) => {
                            assert!(
                                members.contains(&v.as_ref()),
                                "count={count} unexpected member {v:?}"
                            );
                        }
                        other => panic!("expected bulk string for count={count}, got {other:?}"),
                    }
                }
            }
            other => panic!("expected array for count={count}, got {other:?}"),
        }
    }

    // Count larger than the set size returns all members.
    match e.exec(vec![b("SRANDMEMBER"), b("s"), b("10")]) {
        RespValue::Array(Some(items)) => assert_eq!(items.len(), 5),
        other => panic!("expected array for count=10, got {other:?}"),
    }

    // Invalid count is an error.
    assert!(matches!(
        e.exec(vec![b("SRANDMEMBER"), b("s"), b("notint")]),
        RespValue::Error(_)
    ));
}

#[test]
fn srandmember_missing_key_with_count() {
    let e = eng();
    assert_eq!(
        e.exec(vec![b("SRANDMEMBER"), b("missing"), b("2")]),
        RespValue::array(vec![])
    );
}

#[test]
fn spop_missing_key_with_count() {
    let e = eng();
    assert_eq!(
        e.exec(vec![b("SPOP"), b("missing"), b("2")]),
        RespValue::array(vec![])
    );
}

#[test]
fn srandmember_no_count() {
    let e = eng();
    e.exec(vec![b("SADD"), b("s"), b("a")]);
    assert_eq!(
        e.exec(vec![b("SRANDMEMBER"), b("s")]),
        RespValue::bulk(b("a"))
    );
    assert_eq!(
        e.exec(vec![b("SRANDMEMBER"), b("missing")]),
        RespValue::null_bulk()
    );
}

#[test]
fn zset_commands() {
    let e = eng();
    e.exec(vec![b("ZADD"), b("z"), b("1"), b("a"), b("2"), b("b")]);
    assert_eq!(
        e.exec(vec![b("ZSCORE"), b("z"), b("a")]),
        RespValue::bulk(b("1"))
    );
    assert_eq!(e.exec(vec![b("ZCARD"), b("z")]), RespValue::int(2));
    assert_eq!(
        e.exec(vec![b("ZRANGE"), b("z"), b("0"), b("-1")]),
        RespValue::array(vec![RespValue::bulk(b("a")), RespValue::bulk(b("b"))])
    );
    assert_eq!(
        e.exec(vec![b("ZINCRBY"), b("z"), b("1"), b("a")]),
        RespValue::bulk(b("2"))
    );
}

#[test]
fn zadd_parse_error_leaves_mem_and_members_unchanged() {
    let e = eng();
    // Existing zset: one good member charged to mem.
    e.exec(vec![b("ZADD"), b("z"), b("1"), b("a")]);
    let mem_before = e.total_mem();
    // One good pair then one bad score → error, no partial insert, no mem drift.
    let err = match e.exec(vec![
        b("ZADD"),
        b("z"),
        b("2"),
        b("b"),
        b("notafloat"),
        b("c"),
    ]) {
        RespValue::Error(m) => m,
        other => panic!("expected error, got {other:?}"),
    };
    assert!(err.contains("valid float"), "got {err:?}");
    assert_eq!(
        e.total_mem(),
        mem_before,
        "ZADD parse error must leave mem unchanged (before={mem_before}, now={})",
        e.total_mem()
    );
    assert_eq!(e.exec(vec![b("ZCARD"), b("z")]), RespValue::int(1));
    assert_eq!(
        e.exec(vec![b("ZSCORE"), b("z"), b("a")]),
        RespValue::bulk(b("1"))
    );
    assert_eq!(
        e.exec(vec![b("ZSCORE"), b("z"), b("b")]),
        RespValue::null_bulk()
    );

    // New key: parse error must not create the key or charge mem.
    let mem0 = e.total_mem();
    let err = match e.exec(vec![b("ZADD"), b("z2"), b("1"), b("x"), b("bad"), b("y")]) {
        RespValue::Error(m) => m,
        other => panic!("expected error, got {other:?}"),
    };
    assert!(err.contains("valid float"), "got {err:?}");
    assert_eq!(
        e.total_mem(),
        mem0,
        "parse error on new key must not charge mem (before={mem0}, now={})",
        e.total_mem()
    );
    assert_eq!(e.exec(vec![b("EXISTS"), b("z2")]), RespValue::int(0));
}
