use bytes::Bytes;
use jx_engine::{system_now_ms, MaxMemoryPolicy, ShardState};

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

#[test]
fn shard_state_set_get_incr() {
    let mut sh = ShardState::new_empty(MaxMemoryPolicy::NoEviction);
    let now = system_now_ms();
    assert_eq!(
        sh.execute(&[b("SET"), b("k"), b("v")], now).unwrap(),
        jx_proto::RespValue::ok()
    );
    assert_eq!(
        sh.execute(&[b("GET"), b("k")], now).unwrap(),
        jx_proto::RespValue::bulk(b("v"))
    );
    assert_eq!(
        sh.execute(&[b("INCR"), b("n")], now).unwrap(),
        jx_proto::RespValue::int(1)
    );
}
