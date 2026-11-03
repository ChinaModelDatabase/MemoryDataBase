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
        "SCAN" => scan(rt, a),
        _ => rt.exec(args),
    }
}

/// Cross-shard `SCAN cursor [MATCH pattern] [COUNT n]`. Replies
/// `[bulk(next_cursor), [key...]]`; cursor `0` ends the iteration.
fn scan(rt: &ShardRuntime, a: &[Bytes]) -> RespValue {
    let Some(cur) = a.first() else {
        return RespValue::err("ERR wrong number of arguments for 'scan' command");
    };
    let Some(cursor) = std::str::from_utf8(cur)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    else {
        return RespValue::err("ERR invalid cursor");
    };
    if cursor < 0 {
        return RespValue::err("ERR invalid cursor");
    }
    let mut pattern = "*".to_string();
    let mut count = 10usize;
    let mut i = 1;
    while i < a.len() {
        let opt = String::from_utf8_lossy(&a[i]).to_ascii_uppercase();
        match opt.as_str() {
            "MATCH" | "COUNT" => {
                let Some(v) = a.get(i + 1) else {
                    return RespValue::err("ERR syntax error");
                };
                if opt == "MATCH" {
                    pattern = String::from_utf8_lossy(v).to_string();
                } else {
                    let n = std::str::from_utf8(v)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    if n <= 0 {
                        return RespValue::err("ERR syntax error");
                    }
                    count = n as usize;
                }
                i += 2;
            }
            _ => return RespValue::err("ERR syntax error"),
        }
    }
    coordinator::scan(rt, cursor as u64, &pattern, count)
}
