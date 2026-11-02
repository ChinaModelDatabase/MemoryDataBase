use bytes::Bytes;
use jx_proto::RespValue;
use rand::seq::SliceRandom;

use crate::{entry_mem, ShardState};

pub fn del(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    let mut n = 0;
    for k in args {
        if sh.touch_alive(k, now) || sh.dict.contains_key(k) {
            if let Some(old) = sh.dict.remove(k) {
                sh.mem = sh.mem.saturating_sub(entry_mem(k, &old.value));
            }
            n += 1;
        }
    }
    Ok(RespValue::int(n))
}

pub fn exists(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    let mut n = 0;
    for k in args {
        if sh.touch_alive(k, now) {
            n += 1;
        }
    }
    Ok(RespValue::int(n))
}

pub fn key_type(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'type' command",
        ));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::bulk(Bytes::from_static(b"none")));
    }
    Ok(RespValue::bulk(Bytes::from(
        sh.dict[&args[0]].value.type_name(),
    )))
}

pub fn expire(
    sh: &mut ShardState,
    args: &[Bytes],
    now: u64,
    ms: bool,
) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    let n: i64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))?;
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    let deadline = if ms {
        now.saturating_add(n.max(0) as u64)
    } else {
        now.saturating_add((n.max(0) as u64).saturating_mul(1000))
    };
    sh.dict.get_mut(&args[0]).unwrap().meta.expire_at_ms = Some(deadline);
    Ok(RespValue::int(1))
}

/// EXPIREAT key unix-seconds / PEXPIREAT key unix-milliseconds (absolute).
pub fn expire_at(
    sh: &mut ShardState,
    args: &[Bytes],
    now: u64,
    ms: bool,
) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    let n: i64 = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))?;
    let deadline = if ms {
        n.max(0) as u64
    } else {
        (n.max(0) as u64).saturating_mul(1000)
    };
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    sh.dict.get_mut(&args[0]).unwrap().meta.expire_at_ms = Some(deadline);
    Ok(RespValue::int(1))
}

pub fn ttl(
    sh: &mut ShardState,
    args: &[Bytes],
    now: u64,
    ms: bool,
) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(-2));
    }
    match sh.dict[&args[0]].meta.expire_at_ms {
        None => Ok(RespValue::int(-1)),
        Some(exp) => {
            let left = exp.saturating_sub(now);
            Ok(RespValue::int(if ms {
                left as i64
            } else {
                (left / 1000) as i64
            }))
        }
    }
}

pub fn persist(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    let had = sh
        .dict
        .get_mut(&args[0])
        .unwrap()
        .meta
        .expire_at_ms
        .take()
        .is_some();
    Ok(RespValue::int(had as i64))
}

/// RENAME src dst — both keys must live on this shard.
pub fn rename(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'rename' command",
        ));
    }
    let src = &args[0];
    let dst = &args[1];
    if src == dst {
        return if sh.touch_alive(src, now) {
            Ok(RespValue::ok())
        } else {
            Err(RespValue::err("ERR no such key"))
        };
    }
    if !sh.touch_alive(src, now) {
        return Err(RespValue::err("ERR no such key"));
    }
    if let Some(old) = sh.dict.remove(dst) {
        sh.mem = sh.mem.saturating_sub(entry_mem(dst, &old.value));
    }
    if let Some(entry) = sh.dict.remove(src) {
        sh.mem = sh.mem.saturating_sub(entry_mem(src, &entry.value));
        sh.mem += entry_mem(dst, &entry.value);
        sh.dict.insert(dst.clone(), entry);
    }
    Ok(RespValue::ok())
}

/// RANDOMKEY — return a random live key from this shard, or null.
pub fn randomkey(sh: &mut ShardState, _args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    let mut candidates = Vec::new();
    let keys: Vec<Bytes> = sh.dict.keys().cloned().collect();
    for k in keys {
        if sh.touch_alive(&k, now) {
            candidates.push(k);
        }
    }
    if candidates.is_empty() {
        return Ok(RespValue::null_bulk());
    }
    let k = candidates
        .choose(&mut rand::thread_rng())
        .expect("candidates non-empty")
        .clone();
    Ok(RespValue::bulk(k))
}
