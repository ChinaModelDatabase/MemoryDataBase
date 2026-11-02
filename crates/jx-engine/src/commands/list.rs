use std::collections::VecDeque;

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

fn parse_i64(b: &Bytes) -> Result<i64, RespValue> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))
}

fn ensure_list(sh: &mut ShardState, key: &Bytes, now: u64) -> Result<(), RespValue> {
    if sh.touch_alive(key, now) {
        match &sh.dict[key].value {
            JxValue::List(_) => Ok(()),
            _ => Err(wrongtype()),
        }
    } else {
        let value = JxValue::List(VecDeque::new());
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
        .map(|e| matches!(e.value, JxValue::List(ref l) if l.is_empty()))
        .unwrap_or(false)
    {
        sh.mem = sh.mem.saturating_sub(old_mem);
        sh.dict.remove(key);
    }
}

pub fn lpush(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    push(sh, args, now, true)
}

pub fn rpush(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    push(sh, args, now, false)
}

fn push(sh: &mut ShardState, args: &[Bytes], now: u64, left: bool) -> Result<RespValue, RespValue> {
    if args.len() < 2 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    sh.evict_if_needed(&args[0], now)?;
    ensure_list(sh, &args[0], now)?;
    let old_mem = key_mem(sh, &args[0]);
    let len;
    {
        let l = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
            JxValue::List(l) => l,
            _ => unreachable!(),
        };
        for v in &args[1..] {
            if left {
                l.push_front(v.clone());
            } else {
                l.push_back(v.clone());
            }
        }
        len = l.len();
    }
    resync_key_mem(sh, &args[0], old_mem);
    Ok(RespValue::int(len as i64))
}

pub fn lpop(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    pop(sh, args, now, true)
}

pub fn rpop(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    pop(sh, args, now, false)
}

fn pop(sh: &mut ShardState, args: &[Bytes], now: u64, left: bool) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::null_bulk());
    }
    let old_mem = key_mem(sh, &args[0]);
    let (out, empty) = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
        JxValue::List(l) => {
            let popped = if left { l.pop_front() } else { l.pop_back() };
            (
                popped
                    .map(RespValue::bulk)
                    .unwrap_or(RespValue::null_bulk()),
                l.is_empty(),
            )
        }
        _ => return Err(wrongtype()),
    };
    if empty {
        drop_if_empty(sh, &args[0], old_mem);
    } else {
        resync_key_mem(sh, &args[0], old_mem);
    }
    Ok(out)
}

pub fn llen(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    match &sh.dict[&args[0]].value {
        JxValue::List(l) => Ok(RespValue::int(l.len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn lrange(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'lrange' command",
        ));
    }
    let start = parse_i64(&args[1])?;
    let stop = parse_i64(&args[2])?;
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::array(vec![]));
    }
    match &sh.dict[&args[0]].value {
        JxValue::List(l) => {
            let len = l.len() as i64;
            let s = if start < 0 {
                (len + start).max(0)
            } else {
                start.min(len)
            };
            let t = if stop < 0 {
                (len + stop).max(-1)
            } else {
                stop.min(len - 1)
            };
            if s > t {
                return Ok(RespValue::array(vec![]));
            }
            Ok(RespValue::array(
                l.iter()
                    .skip(s as usize)
                    .take((t - s + 1) as usize)
                    .cloned()
                    .map(RespValue::bulk)
                    .collect(),
            ))
        }
        _ => Err(wrongtype()),
    }
}

pub fn lindex(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    let idx = parse_i64(&args[1])?;
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::null_bulk());
    }
    match &sh.dict[&args[0]].value {
        JxValue::List(l) => {
            let len = l.len() as i64;
            let i = if idx < 0 { len + idx } else { idx };
            if i < 0 || i >= len {
                return Ok(RespValue::null_bulk());
            }
            Ok(RespValue::bulk(l[i as usize].clone()))
        }
        _ => Err(wrongtype()),
    }
}

pub fn lset(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'lset' command",
        ));
    }
    let idx = parse_i64(&args[1])?;
    if !sh.touch_alive(&args[0], now) {
        return Err(RespValue::err("ERR no such key"));
    }
    let old_mem = key_mem(sh, &args[0]);
    match &mut sh.dict.get_mut(&args[0]).unwrap().value {
        JxValue::List(l) => {
            let len = l.len() as i64;
            let i = if idx < 0 { len + idx } else { idx };
            if i < 0 || i >= len {
                return Err(RespValue::err("ERR index out of range"));
            }
            l[i as usize] = args[2].clone();
            resync_key_mem(sh, &args[0], old_mem);
            Ok(RespValue::ok())
        }
        _ => Err(wrongtype()),
    }
}
