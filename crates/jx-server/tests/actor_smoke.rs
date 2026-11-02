use bytes::Bytes;
use jx_engine::actor::spawn_shard_runtime;
use jx_engine::system_now_ms;
use jx_proto::RespValue;
use std::sync::Arc;
use std::time::Duration;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn rt(n: usize) -> jx_engine::actor::ShardRuntime {
    spawn_shard_runtime(n, 1024, system_now_ms)
}

#[tokio::test]
async fn background_expire_cycle_removes_expired_keys() {
    let engine = Arc::new(rt(2));
    let _bg = jx_server::background::spawn_background(engine.clone());
    engine.exec(vec![b("SET"), b("expiring"), b("v")]);
    engine.exec(vec![b("SET"), b("kept"), b("v2")]);
    engine.exec(vec![
        b("EXPIRE"),
        Bytes::from_static(b"expiring"),
        Bytes::from_static(b"0"),
    ]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    // DBSIZE does not lazy-evict; only the background cycle (or touch on
    // access) removes expired keys — so this asserts the background task ran.
    let dbsize = engine.exec(vec![b("DBSIZE")]);
    assert_eq!(
        dbsize,
        RespValue::int(1),
        "background cycle should remove expiring"
    );
    let got = engine.exec(vec![b("GET"), Bytes::from_static(b"expiring")]);
    assert_eq!(got, RespValue::null_bulk(), "expired key should be gone");
    let kept = engine.exec(vec![b("GET"), Bytes::from_static(b"kept")]);
    assert_eq!(kept, RespValue::bulk(Bytes::from_static(b"v2")));
}

#[tokio::test]
async fn shard_runtime_over_server_engine_path() {
    // Multi-shard MGET through ShardRuntime::exec (server dispatch path).
    let engine = Arc::new(rt(4));
    for i in 0..20 {
        let resp = engine.exec(vec![
            b("SET"),
            Bytes::from(format!("key{i}")),
            Bytes::from(format!("v{i}")),
        ]);
        assert_eq!(resp, RespValue::ok());
    }
    let mut args = vec![b("MGET")];
    args.extend((0..20).map(|i| Bytes::from(format!("key{i}"))));
    let r = engine.exec(args);
    match r {
        RespValue::Array(Some(items)) => {
            assert_eq!(items.len(), 20);
            for (i, item) in items.iter().enumerate() {
                assert_eq!(*item, RespValue::bulk(Bytes::from(format!("v{i}"))));
            }
        }
        other => panic!("bad mget via ShardRuntime: {other:?}"),
    }
}
