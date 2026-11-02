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

#[test]
fn ping_returns_simple_string() {
    let e = eng();
    assert_eq!(
        e.exec(vec![b("PING")]),
        RespValue::SimpleString("PONG".into())
    );
}

#[test]
fn set_get_del() {
    let e = eng();
    assert_eq!(e.exec(vec![b("SET"), b("k"), b("v")]), RespValue::ok());
    assert_eq!(e.exec(vec![b("GET"), b("k")]), RespValue::bulk(b("v")));
    assert_eq!(e.exec(vec![b("DEL"), b("k")]), RespValue::int(1));
    assert_eq!(e.exec(vec![b("GET"), b("k")]), RespValue::null_bulk());
}

#[test]
fn incr_decr_incrby_decrby() {
    let e = eng();
    e.exec(vec![b("SET"), b("n"), b("10")]);
    assert_eq!(e.exec(vec![b("INCR"), b("n")]), RespValue::int(11));
    assert_eq!(e.exec(vec![b("DECR"), b("n")]), RespValue::int(10));
    assert_eq!(
        e.exec(vec![b("INCRBY"), b("n"), b("5")]),
        RespValue::int(15)
    );
    assert_eq!(
        e.exec(vec![b("DECRBY"), b("n"), b("5")]),
        RespValue::int(10)
    );
}

#[test]
fn set_append_strlen_getrange() {
    let e = eng();
    e.exec(vec![b("SET"), b("s"), b("hello")]);
    assert_eq!(
        e.exec(vec![b("APPEND"), b("s"), b(" world")]),
        RespValue::int(11)
    );
    assert_eq!(e.exec(vec![b("STRLEN"), b("s")]), RespValue::int(11));
    assert_eq!(
        e.exec(vec![b("GETRANGE"), b("s"), b("0"), b("4")]),
        RespValue::bulk(b("hello"))
    );
}

#[test]
fn mget_mset_setnx() {
    let e = eng();
    e.exec(vec![b("MSET"), b("a"), b("1"), b("b"), b("2")]);
    let r = e.exec(vec![b("MGET"), b("a"), b("missing"), b("b")]);
    assert_eq!(
        r,
        RespValue::array(vec![
            RespValue::bulk(b("1")),
            RespValue::null_bulk(),
            RespValue::bulk(b("2"))
        ])
    );
    assert_eq!(e.exec(vec![b("SETNX"), b("a"), b("x")]), RespValue::int(0));
    assert_eq!(e.exec(vec![b("SETNX"), b("c"), b("3")]), RespValue::int(1));
}

#[test]
fn mget_propagates_wrongtype_only_for_wrongtype_keys() {
    let e = eng();
    e.exec(vec![b("SET"), b("s"), b("str")]);
    e.exec(vec![b("HSET"), b("h"), b("f"), b("v")]);
    // Wrong-type key must surface WRONGTYPE (not become null).
    let err = match e.exec(vec![b("MGET"), b("s"), b("h"), b("missing")]) {
        RespValue::Error(m) => m,
        other => panic!("expected error, got {other:?}"),
    };
    assert!(
        err.starts_with("WRONGTYPE"),
        "MGET must propagate WRONGTYPE, got {err:?}"
    );
    // Missing keys alone are still nulls.
    let r = e.exec(vec![b("MGET"), b("s"), b("missing")]);
    assert_eq!(
        r,
        RespValue::array(vec![RespValue::bulk(b("str")), RespValue::null_bulk()])
    );
}

#[test]
fn decrby_i64_min_rejects_overflow() {
    let e = eng();
    e.exec(vec![b("SET"), b("n"), b("0")]);
    let err = match e.exec(vec![b("DECRBY"), b("n"), b("-9223372036854775808")]) {
        RespValue::Error(m) => m,
        other => panic!("expected error, got {other:?}"),
    };
    assert!(
        err.contains("overflow") || err.contains("out of range"),
        "DECRBY i64::MIN must overflow-check, got {err:?}"
    );
}

#[test]
fn append_preserves_existing_ttl() {
    let e = eng();
    e.exec(vec![b("SET"), b("k"), b("ab"), b("EX"), b("100")]);
    let ttl_before = e.exec(vec![b("TTL"), b("k")]);
    assert!(matches!(ttl_before, RespValue::Integer(n) if (99..=100).contains(&n)));
    e.exec(vec![b("APPEND"), b("k"), b("cd")]);
    let ttl_after = e.exec(vec![b("TTL"), b("k")]);
    assert!(
        matches!(ttl_after, RespValue::Integer(n) if (95..=100).contains(&n)),
        "APPEND must preserve TTL, got {ttl_after:?}"
    );
    assert_eq!(e.exec(vec![b("GET"), b("k")]), RespValue::bulk(b("abcd")));
}

#[test]
fn setex_psetex_msetnx_select() {
    let e = eng();
    assert_eq!(
        e.exec(vec![b("SETEX"), b("a"), b("50"), b("v")]),
        RespValue::ok()
    );
    assert_eq!(e.exec(vec![b("GET"), b("a")]), RespValue::bulk(b("v")));
    let ttl = e.exec(vec![b("TTL"), b("a")]);
    assert!(
        matches!(ttl, RespValue::Integer(n) if (45..=50).contains(&n)),
        "ttl={ttl:?}"
    );
    assert_eq!(
        e.exec(vec![b("PSETEX"), b("b"), b("50000"), b("w")]),
        RespValue::ok()
    );
    assert_eq!(e.exec(vec![b("GET"), b("b")]), RespValue::bulk(b("w")));

    // MSETNX: all free → 1; any existing → 0 and no partial write.
    assert_eq!(
        e.exec(vec![b("MSETNX"), b("n1"), b("1"), b("n2"), b("2")]),
        RespValue::int(1)
    );
    assert_eq!(
        e.exec(vec![b("MSETNX"), b("n1"), b("x"), b("n3"), b("3")]),
        RespValue::int(0)
    );
    assert_eq!(e.exec(vec![b("GET"), b("n1")]), RespValue::bulk(b("1")));
    assert_eq!(e.exec(vec![b("EXISTS"), b("n3")]), RespValue::int(0));

    // SELECT: db 0 OK, other dbs error.
    assert_eq!(e.exec(vec![b("SELECT"), b("0")]), RespValue::ok());
    let err = match e.exec(vec![b("SELECT"), b("1")]) {
        RespValue::Error(m) => m,
        other => panic!("expected error, got {other:?}"),
    };
    assert!(err.contains("out of range"), "got {err:?}");
}

#[test]
fn rename_and_randomkey() {
    let e = eng();
    e.exec(vec![b("SET"), b("src"), b("val")]);
    assert_eq!(
        e.exec(vec![b("RENAME"), b("src"), b("dst")]),
        RespValue::ok()
    );
    assert_eq!(e.exec(vec![b("GET"), b("dst")]), RespValue::bulk(b("val")));
    assert_eq!(e.exec(vec![b("EXISTS"), b("src")]), RespValue::int(0));
    // Missing src errors.
    let err = match e.exec(vec![b("RENAME"), b("nope"), b("x")]) {
        RespValue::Error(m) => m,
        other => panic!("expected error, got {other:?}"),
    };
    assert!(err.contains("no such key"), "got {err:?}");
    // Overwrite existing dst.
    e.exec(vec![b("SET"), b("d2"), b("old")]);
    e.exec(vec![b("SET"), b("s2"), b("new")]);
    assert_eq!(e.exec(vec![b("RENAME"), b("s2"), b("d2")]), RespValue::ok());
    assert_eq!(e.exec(vec![b("GET"), b("d2")]), RespValue::bulk(b("new")));

    // RANDOMKEY returns a live key or null on empty db.
    match e.exec(vec![b("RANDOMKEY")]) {
        RespValue::BulkString(Some(k)) => {
            assert_eq!(e.exec(vec![b("EXISTS"), k]), RespValue::int(1));
        }
        RespValue::BulkString(None) => {}
        other => panic!("unexpected RANDOMKEY reply {other:?}"),
    }
    let empty = eng();
    assert_eq!(empty.exec(vec![b("RANDOMKEY")]), RespValue::null_bulk());
}

#[test]
fn info_reports_expires_count() {
    let e = eng();
    e.exec(vec![b("SET"), b("k"), b("v"), b("EX"), b("100")]);
    e.exec(vec![b("SET"), b("plain"), b("v")]);
    let r = e.exec(vec![b("INFO")]);
    let RespValue::BulkString(Some(text)) = r else {
        panic!("INFO must be bulk");
    };
    let text = String::from_utf8_lossy(&text).to_string();
    assert!(
        text.contains("expires=1"),
        "INFO must count expiring keys, got {text:?}"
    );
}

// Task 3 seeded a Hash entry directly because HSET was unimplemented until
// Task 4. HSET now creates hashes; MULTI remains unimplemented. WRONGTYPE on
// GET against a hash is still covered.
#[test]
fn wrongtype_and_unsupported() {
    let e = eng();
    assert_eq!(
        e.exec(vec![b("HSET"), b("h"), b("f"), b("1")]),
        RespValue::int(1)
    );
    let err = match e.exec(vec![b("MULTI")]) {
        RespValue::Error(m) => m,
        other => panic!("expected error, got {other:?}"),
    };
    assert!(err.contains("not supported"), "got {err:?}");
    let err = match e.exec(vec![b("GET"), b("h")]) {
        RespValue::Error(m) => m,
        other => panic!("expected error, got {other:?}"),
    };
    assert!(err.starts_with("WRONGTYPE"), "got {err:?}");
}

#[test]
fn expire_ttl_persist_exists_type() {
    let e = eng();
    e.exec(vec![b("SET"), b("k"), b("v"), b("EX"), b("100")]);
    let ttl = e.exec(vec![b("TTL"), b("k")]);
    assert!(matches!(ttl, RespValue::Integer(n) if (99..=100).contains(&n)));
    assert_eq!(
        e.exec(vec![b("TYPE"), b("k")]),
        RespValue::bulk(b("string"))
    );
    assert_eq!(e.exec(vec![b("PERSIST"), b("k")]), RespValue::int(1));
    assert_eq!(e.exec(vec![b("TTL"), b("k")]), RespValue::int(-1));
    assert_eq!(e.exec(vec![b("EXISTS"), b("k")]), RespValue::int(1));
}

// silence unused import warning when MaxMemoryPolicy not needed in some builds
#[allow(dead_code)]
fn _use_policy() -> MaxMemoryPolicy {
    MaxMemoryPolicy::NoEviction
}
