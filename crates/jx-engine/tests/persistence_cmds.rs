use bytes::Bytes;
use jx_engine::actor::spawn_shard_runtime;
use jx_engine::system_now_ms;

/// ShardRuntime `exec` must reject SAVE-family commands by design: persistence
/// is intercepted at the jx-server conn layer so the engine stays free of a
/// jx-persist dependency. The error message is the stable contract.
#[test]
fn save_family_rejected_by_engine_with_stable_error() {
    let e = spawn_shard_runtime(1, 1024, system_now_ms);
    for cmd in ["SAVE", "BGSAVE", "LASTSAVE"] {
        let err = e.exec(vec![Bytes::from_static(cmd.as_bytes())]);
        let msg = match &err {
            jx_proto::RespValue::Error(s) => s.clone(),
            other => panic!("{cmd}: expected Error resp, got {other:?}"),
        };
        assert!(
            msg.contains("unknown command"),
            "{cmd}: message must say unknown command, got {msg:?}"
        );
        assert!(
            msg.contains(cmd),
            "{cmd}: message must name the command, got {msg:?}"
        );
    }
}
