use std::collections::HashMap;

use bytes::Bytes;
use jx_proto::RespValue;

use crate::{
    key_mem, resync_key_mem,
    types::{entry_mem, Entry, JxValue, KeyMeta},
    ShardState,
};

fn wrongtype() -> RespValue {
    RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn ensure_hash(sh: &mut ShardState, key: &Bytes, now: u64) -> Result<(), RespValue> {
    if sh.touch_alive(key, now) {
        match &sh.dict[key].value {
            JxValue::Hash(_) => Ok(()),
            _ => Err(wrongtype()),
        }
    } else {
        let value = JxValue::Hash(HashMap::new());
        sh.mem += entry_mem(key, &value);
        sh.dict.insert(
            key.clone(),
            Entry {
                value,
                meta: KeyMeta {
                    expire_at_ms: None,
                    last_access_ms: now,
                    freq: 1,
                },
            },
        );
        Ok(())
    }
}

fn drop_if_empty(sh: &mut ShardState, key: &Bytes, old_mem: usize) {
    if sh
        .dict
        .get(key)
        .map(|e| matches!(e.value, JxValue::Hash(ref h) if h.is_empty()))
        .unwrap_or(false)
    {
        sh.mem = sh.mem.saturating_sub(old_mem);
        sh.dict.remove(key);
    }
}

pub fn hset(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'hset' command",
        ));
    }
    sh.evict_if_needed(&args[0], now)?;
    ensure_hash(sh, &args[0], now)?;
    let old_mem = key_mem(sh, &args[0]);
    let mut added = 0;
    {
        let h = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
            JxValue::Hash(h) => h,
            _ => unreachable!(),
        };
        for pair in args[1..].chunks(2) {
            if h.insert(pair[0].clone(), pair[1].clone()).is_none() {
                added += 1;
            }
        }
    }
    resync_key_mem(sh, &args[0], old_mem);
    Ok(RespValue::int(added))
}

pub fn hget(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'hget' command",
        ));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::null_bulk());
    }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(match h.get(&args[1]) {
            Some(v) => RespValue::bulk(v.clone()),
            None => RespValue::null_bulk(),
        }),
        _ => Err(wrongtype()),
    }
}

pub fn hmget(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'hmget' command",
        ));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::array(vec![
            RespValue::null_bulk();
            args.len() - 1
        ]));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::array(
            args[1..]
                .iter()
                .map(|f| match h.get(f) {
                    Some(v) => RespValue::bulk(v.clone()),
                    None => RespValue::null_bulk(),
                })
                .collect(),
        )),
        _ => Err(wrongtype()),
    }
}

pub fn hdel(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'hdel' command",
        ));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    let old_mem = key_mem(sh, &args[0]);
    let mut n = 0;
    let empty = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
        JxValue::Hash(h) => {
            for f in &args[1..] {
                if h.remove(f).is_some() {
                    n += 1;
                }
            }
            h.is_empty()
        }
        _ => return Err(wrongtype()),
    };
    if empty {
        drop_if_empty(sh, &args[0], old_mem);
    } else {
        resync_key_mem(sh, &args[0], old_mem);
    }
    Ok(RespValue::int(n))
}

pub fn hexists(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::int(h.contains_key(&args[1]) as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn hlen(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::int(h.len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn hkeys(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::array(vec![]));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::array(
            h.keys().cloned().map(RespValue::bulk).collect(),
        )),
        _ => Err(wrongtype()),
    }
}

pub fn hvals(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::array(vec![]));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => Ok(RespValue::array(
            h.values().cloned().map(RespValue::bulk).collect(),
        )),
        _ => Err(wrongtype()),
    }
}

pub fn hgetall(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::array(vec![]));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Hash(h) => {
            let mut out = Vec::with_capacity(h.len() * 2);
            for (k, v) in h {
                out.push(RespValue::bulk(k.clone()));
                out.push(RespValue::bulk(v.clone()));
            }
            Ok(RespValue::array(out))
        }
        _ => Err(wrongtype()),
    }
}

pub fn hincrby(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'hincrby' command",
        ));
    }
    let delta: i64 = std::str::from_utf8(&args[2])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))?;
    sh.evict_if_needed(&args[0], now)?;
    ensure_hash(sh, &args[0], now)?;
    let old_mem = key_mem(sh, &args[0]);
    let new;
    {
        let h = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
            JxValue::Hash(h) => h,
            _ => unreachable!(),
        };
        let cur = match h.get(&args[1]) {
            None => 0i64,
            Some(v) => std::str::from_utf8(v)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| RespValue::err("ERR hash value is not an integer"))?,
        };
        new = cur.saturating_add(delta);
        h.insert(args[1].clone(), Bytes::from(new.to_string()));
    }
    resync_key_mem(sh, &args[0], old_mem);
    Ok(RespValue::int(new))
}
