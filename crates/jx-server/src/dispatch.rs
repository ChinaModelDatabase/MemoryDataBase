use bytes::Bytes;
use jx_engine::actor::ShardRuntime;
use jx_engine::coordinator;
use jx_proto::RespValue;

/// Dispatch a command through the shard actor runtime.
///
/// Multi-shard commands (MGET/KEYS/FLUSHALL/DBSIZE/INFO) route directly to
/// `jx_engine::coordinator` APIs (the same implementation `ShardRuntime::exec`
/// delegates to). Everything else goes to `rt.exec` for key-based routing.
pub fn dispatch(rt: &ShardRuntime, args: Vec<Bytes>) -> RespValue {
    if args.is_empty() {
        return RespValue::err("ERR empty command");
    }
    let name = std::str::from_utf8(&args[0])
        .unwrap_or("")
        .to_ascii_uppercase();
    let a = &args[1..];
    match name.as_str() {
        "MGET" => coordinator::mget_coalesced(rt, a),
        "KEYS" => {
            let pat = a
                .first()
                .cloned()
                .unwrap_or_else(|| Bytes::from_static(b"*"));
            coordinator::keys_glob(rt, &pat)
        }
        "FLUSHALL" | "FLUSHDB" => coordinator::flushall(rt),
        "DBSIZE" => coordinator::dbsize(rt),
        "INFO" => coordinator::info(rt),
        _ => rt.exec(args),
    }
}
