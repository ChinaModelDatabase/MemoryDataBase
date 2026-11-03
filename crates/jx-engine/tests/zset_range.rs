use bytes::Bytes;
use jx_engine::actor::{spawn_shard_runtime, ShardRuntime};
use jx_engine::system_now_ms;
use jx_proto::RespValue;

fn eng() -> ShardRuntime {
    spawn_shard_runtime(2, 1024, system_now_ms)
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn zadd_fixture(e: &ShardRuntime) {
    assert_eq!(
        e.exec(vec![b("ZADD"), b("z"), b("1"), b("one"), b("2"), b("two")]),
        RespValue::int(2)
    );
    assert_eq!(
        e.exec(vec![b("ZADD"), b("z"), b("3"), b("three")]),
        RespValue::int(1)
    );
}

#[test]
fn zrange_withscores_returns_member_score_pairs() {
    let e = eng();
    zadd_fixture(&e);
    assert_eq!(
        e.exec(vec![b("ZRANGE"), b("z"), b("0"), b("-1"), b("WITHSCORES")]),
        RespValue::array(vec![
            RespValue::bulk(b("one")),
            RespValue::bulk(b("1")),
            RespValue::bulk(b("two")),
            RespValue::bulk(b("2")),
            RespValue::bulk(b("three")),
            RespValue::bulk(b("3")),
        ])
    );
}

#[test]
fn zrange_without_withscores_returns_members_only() {
    let e = eng();
    zadd_fixture(&e);
    assert_eq!(
        e.exec(vec![b("ZRANGE"), b("z"), b("0"), b("-1")]),
        RespValue::array(vec![
            RespValue::bulk(b("one")),
            RespValue::bulk(b("two")),
            RespValue::bulk(b("three")),
        ])
    );
}

#[test]
fn zrange_rejects_unknown_trailing_option() {
    let e = eng();
    zadd_fixture(&e);
    let resp = e.exec(vec![b("ZRANGE"), b("z"), b("0"), b("-1"), b("BOGUS")]);
    assert!(
        matches!(&resp, RespValue::Error(m) if m.contains("wrong number of arguments")),
        "got {resp:?}"
    );
}

#[test]
fn zrangebyscore_accepts_min_max_only() {
    let e = eng();
    zadd_fixture(&e);
    assert_eq!(
        e.exec(vec![b("ZRANGEBYSCORE"), b("z"), b("0"), b("10")]),
        RespValue::array(vec![
            RespValue::bulk(b("one")),
            RespValue::bulk(b("two")),
            RespValue::bulk(b("three")),
        ])
    );
}

#[test]
fn zrangebyscore_withscores_includes_scores() {
    let e = eng();
    zadd_fixture(&e);
    assert_eq!(
        e.exec(vec![
            b("ZRANGEBYSCORE"),
            b("z"),
            b("0"),
            b("10"),
            b("WITHSCORES")
        ]),
        RespValue::array(vec![
            RespValue::bulk(b("one")),
            RespValue::bulk(b("1")),
            RespValue::bulk(b("two")),
            RespValue::bulk(b("2")),
            RespValue::bulk(b("three")),
            RespValue::bulk(b("3")),
        ])
    );
}

#[test]
fn zrangebyscore_limit_applies_offset_and_count() {
    let e = eng();
    zadd_fixture(&e);
    assert_eq!(
        e.exec(vec![
            b("ZRANGEBYSCORE"),
            b("z"),
            b("0"),
            b("10"),
            b("LIMIT"),
            b("1"),
            b("1")
        ]),
        RespValue::array(vec![RespValue::bulk(b("two"))])
    );
}

#[test]
fn zrangebyscore_limit_with_withscores_includes_scores() {
    let e = eng();
    zadd_fixture(&e);
    assert_eq!(
        e.exec(vec![
            b("ZRANGEBYSCORE"),
            b("z"),
            b("-inf"),
            b("+inf"),
            b("WITHSCORES"),
            b("LIMIT"),
            b("0"),
            b("2")
        ]),
        RespValue::array(vec![
            RespValue::bulk(b("one")),
            RespValue::bulk(b("1")),
            RespValue::bulk(b("two")),
            RespValue::bulk(b("2")),
        ])
    );
}

#[test]
fn zrangebyscore_rejects_unknown_trailing_option() {
    let e = eng();
    zadd_fixture(&e);
    let resp = e.exec(vec![
        b("ZRANGEBYSCORE"),
        b("z"),
        b("0"),
        b("10"),
        b("BOGUS"),
    ]);
    assert!(
        matches!(&resp, RespValue::Error(_)),
        "unknown option must error, got {resp:?}"
    );
}
