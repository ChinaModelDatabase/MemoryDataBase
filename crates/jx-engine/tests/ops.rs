use bytes::Bytes;
use jx_engine::actor::{spawn_shard_runtime, ShardRuntime};
use jx_engine::system_now_ms;
use jx_proto::RespValue;

fn rt(nshards: usize) -> ShardRuntime {
    spawn_shard_runtime(nshards, 1024, system_now_ms)
}
fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

#[test]
fn info_and_config_and_flush() {
    let e = rt(1);
    let info = e.exec(vec![b("INFO")]);
    match info {
        RespValue::BulkString(Some(t)) => {
            let text = String::from_utf8_lossy(&t);
            assert!(text.contains("MemoryDataBase_version"), "info text: {text}");
        }
        other => panic!("bad info {other:?}"),
    }
    e.exec(vec![b("SET"), b("k"), b("v")]);
    e.exec(vec![b("FLUSHALL")]);
    assert_eq!(e.exec(vec![b("EXISTS"), b("k")]), RespValue::int(0));
    e.exec(vec![b("SET"), b("a"), b("1"), b("EX"), b("100")]);
    let keys = e.exec(vec![b("KEYS"), b("*")]);
    match keys {
        RespValue::Array(Some(items)) => {
            assert!(items
                .iter()
                .any(|k| matches!(k, RespValue::BulkString(Some(b)) if b.as_ref() == b"a")));
        }
        other => panic!("bad keys {other:?}"),
    }
}

#[test]
fn keys_glob_matches_and_excludes() {
    let e = rt(2);
    for k in ["user:1", "user:2", "session:a", "other"] {
        e.exec(vec![b("SET"), b(k), b("v")]);
    }
    let keys = e.exec(vec![b("KEYS"), b("user:*")]);
    match keys {
        RespValue::Array(Some(items)) => {
            let mut names: Vec<String> = items
                .iter()
                .map(|x| match x {
                    RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                    _ => panic!("bad item {x:?}"),
                })
                .collect();
            names.sort();
            assert_eq!(names, vec!["user:1".to_string(), "user:2".to_string()]);
        }
        other => panic!("bad keys {other:?}"),
    }
    let keys = e.exec(vec![b("KEYS"), b("user:?")]);
    match keys {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 2, "? matches single-char suffixes");
        }
        other => panic!("bad keys {other:?}"),
    }
    let keys = e.exec(vec![b("KEYS"), b("user:??")]);
    match keys {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 0, "?? does not match single-char suffixes");
        }
        other => panic!("bad keys {other:?}"),
    }
}

#[test]
fn config_set_get_maxmemory() {
    let e = rt(1);
    let r = e.exec(vec![b("CONFIG"), b("SET"), b("maxmemory"), b("1048576")]);
    assert_eq!(r, RespValue::ok());
    assert_eq!(e.maxmemory(), Some(1048576));

    let r = e.exec(vec![
        b("CONFIG"),
        b("SET"),
        b("maxmemory-policy"),
        b("allkeys-lru"),
    ]);
    assert_eq!(r, RespValue::ok());

    let r = e.exec(vec![b("CONFIG"), b("GET"), b("maxmemory")]);
    match r {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::bulk(b("maxmemory")));
            assert_eq!(items[1], RespValue::bulk(b("1048576")));
        }
        other => panic!("bad config get {other:?}"),
    }

    let r = e.exec(vec![b("CONFIG"), b("GET"), b("maxmemory-policy")]);
    match r {
        RespValue::Array(Some(items)) => {
            assert_eq!(items[1], RespValue::bulk(b("allkeys-lru")));
        }
        other => panic!("bad config get policy {other:?}"),
    }

    assert_eq!(e.exec(vec![b("CONFIG"), b("RESETSTAT")]), RespValue::ok());
}

#[test]
fn flushdb_clears_like_flushall() {
    let e = rt(2);
    e.exec(vec![b("SET"), b("x"), b("1")]);
    assert_eq!(e.exec(vec![b("FLUSHDB")]), RespValue::ok());
    assert_eq!(e.exec(vec![b("EXISTS"), b("x")]), RespValue::int(0));
}
