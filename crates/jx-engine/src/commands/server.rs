use bytes::Bytes;
use jx_proto::RespValue;

use crate::{MaxMemoryPolicy, ShardState};

/// Shard-local INFO: stats for this shard only.
pub fn info(sh: &ShardState, _args: &[Bytes], _now: u64) -> Result<RespValue, RespValue> {
    Ok(info_text(sh.dict.len(), expires_count(sh), sh.mem))
}

pub(crate) fn info_text(keys: usize, expires: usize, mem: usize) -> RespValue {
    let text = format!(
        "# Server\r\nMemoryDataBase_version:0.1.0\r\nos:{}\r\narch:{}\r\n# Memory\r\nused_memory:{}\r\n# Keyspace\r\ndb0:keys={keys},expires={expires}\r\n",
        std::env::consts::OS,
        std::env::consts::ARCH,
        mem,
    );
    RespValue::bulk(Bytes::from(text))
}

pub(crate) fn expires_count(sh: &ShardState) -> usize {
    sh.dict
        .values()
        .filter(|e| e.meta.expire_at_ms.is_some())
        .count()
}

/// Shard-local FLUSHALL: clear this shard only.
pub fn flushall(sh: &mut ShardState, _args: &[Bytes], _now: u64) -> Result<RespValue, RespValue> {
    sh.dict.clear();
    sh.mem = 0;
    Ok(RespValue::ok())
}

/// Collect live keys from this shard matching the glob pattern.
pub fn collect_keys(sh: &mut ShardState, pat: &str, now: u64) -> Vec<Bytes> {
    let mut out = Vec::new();
    let ks: Vec<Bytes> = sh.dict.keys().cloned().collect();
    for k in ks {
        if sh.touch_alive(&k, now) {
            let s = String::from_utf8_lossy(&k);
            if glob_match(pat, &s) {
                out.push(k);
            }
        }
    }
    out
}

/// Shard-local KEYS: keys on this shard only.
pub fn keys(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    let pat = args
        .first()
        .map(|p| String::from_utf8_lossy(p).to_string())
        .unwrap_or_else(|| "*".into());
    let out = collect_keys(sh, &pat, now);
    Ok(RespValue::array(
        out.into_iter().map(RespValue::bulk).collect(),
    ))
}

/// Shard-local CONFIG: operate on this shard's maxmemory/policy.
pub fn config(sh: &mut ShardState, args: &[Bytes], _now: u64) -> Result<RespValue, RespValue> {
    if args.is_empty() {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'config' command",
        ));
    }
    let sub = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    match sub.as_str() {
        "GET" => config_get(sh, &args[1..]),
        "SET" => config_set(sh, &args[1..]),
        "RESETSTAT" => Ok(RespValue::ok()),
        other => Err(RespValue::err(format!(
            "ERR unsupported CONFIG subcommand '{other}'"
        ))),
    }
}

fn config_get(sh: &ShardState, args: &[Bytes]) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'config|get' command",
        ));
    }
    let param = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    match param.as_str() {
        "maxmemory" => {
            let v = sh
                .maxmemory
                .map(|m| m.to_string())
                .unwrap_or_else(|| "0".into());
            Ok(config_pair(&param, &v))
        }
        "maxmemory-policy" => Ok(config_pair(&param, sh.policy.as_str())),
        "appendonly" => Ok(config_pair(&param, "no")),
        other => Err(RespValue::err(format!(
            "ERR unsupported CONFIG parameter '{other}'"
        ))),
    }
}

fn config_set(sh: &mut ShardState, args: &[Bytes]) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'config|set' command",
        ));
    }
    let param = String::from_utf8_lossy(&args[0]).to_ascii_lowercase();
    let val = String::from_utf8_lossy(&args[1]).to_string();
    match param.as_str() {
        "maxmemory" => {
            let n: usize = val
                .parse()
                .map_err(|_| RespValue::err("ERR value is not an integer or out of range"))?;
            sh.maxmemory = if n == 0 { None } else { Some(n) };
            Ok(RespValue::ok())
        }
        "maxmemory-policy" => {
            let policy = MaxMemoryPolicy::parse(&val)
                .ok_or_else(|| RespValue::err(format!("ERR invalid maxmemory-policy '{val}'")))?;
            // Policy-only update: never materialize maxmemory=Some(0) as a limit.
            sh.policy = policy;
            Ok(RespValue::ok())
        }
        "appendonly" => Ok(RespValue::ok()),
        other => Err(RespValue::err(format!(
            "ERR unsupported CONFIG parameter '{other}'"
        ))),
    }
}

fn config_pair(param: &str, val: &str) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(Bytes::copy_from_slice(param.as_bytes())),
        RespValue::bulk(Bytes::copy_from_slice(val.as_bytes())),
    ])
}

/// Tiny glob matcher supporting Redis-style `*` and `?`.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    fn m(p: &[char], t: &[char]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            '*' => {
                let mut i = 0;
                loop {
                    if m(&p[1..], &t[i..]) {
                        return true;
                    }
                    if i >= t.len() {
                        return false;
                    }
                    i += 1;
                }
            }
            '?' => !t.is_empty() && m(&p[1..], &t[1..]),
            c => !t.is_empty() && t[0] == c && m(&p[1..], &t[1..]),
        }
    }
    m(&p, &t)
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn glob_basics() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:1"));
        assert!(!glob_match("user:*", "session:1"));
        assert!(glob_match("user:?", "user:a"));
        assert!(!glob_match("user:?", "user:ab"));
        assert!(glob_match("a*c", "abc"));
        assert!(glob_match("a*c", "ac"));
        assert!(!glob_match("a*c", "abd"));
    }
}
