use std::collections::HashSet;

use bytes::Bytes;
use jx_proto::RespValue;
use rand::seq::SliceRandom;

use crate::{
    key_mem, resync_key_mem,
    types::{entry_mem, Entry, JxValue, KeyMeta},
    ShardState,
};

fn wrongtype() -> RespValue {
    RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn ensure_set(sh: &mut ShardState, key: &Bytes, now: u64) -> Result<(), RespValue> {
    if sh.touch_alive(key, now) {
        match &sh.dict[key].value {
            JxValue::Set(_) => Ok(()),
            _ => Err(wrongtype()),
        }
    } else {
        let value = JxValue::Set(HashSet::new());
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
        .map(|e| matches!(e.value, JxValue::Set(ref s) if s.is_empty()))
        .unwrap_or(false)
    {
        sh.mem = sh.mem.saturating_sub(old_mem);
        sh.dict.remove(key);
    }
}

pub fn sadd(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'sadd' command",
        ));
    }
    sh.evict_if_needed(&args[0], now)?;
    ensure_set(sh, &args[0], now)?;
    let old_mem = key_mem(sh, &args[0]);
    let mut n = 0;
    {
        let s = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
            JxValue::Set(s) => s,
            _ => unreachable!(),
        };
        for v in &args[1..] {
            if s.insert(v.clone()) {
                n += 1;
            }
        }
    }
    resync_key_mem(sh, &args[0], old_mem);
    Ok(RespValue::int(n))
}

pub fn srem(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'srem' command",
        ));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    let old_mem = key_mem(sh, &args[0]);
    let mut n = 0;
    let empty = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
        JxValue::Set(s) => {
            for v in &args[1..] {
                if s.remove(v) {
                    n += 1;
                }
            }
            s.is_empty()
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

pub fn sismember(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Set(s) => Ok(RespValue::int(s.contains(&args[1]) as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn scard(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Set(s) => Ok(RespValue::int(s.len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn smembers(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::array(vec![]));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Set(s) => Ok(RespValue::array(
            s.iter().cloned().map(RespValue::bulk).collect(),
        )),
        _ => Err(wrongtype()),
    }
}

pub fn spop(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.is_empty() || args.len() > 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'spop' command",
        ));
    }
    if !sh.touch_alive(&args[0], now) {
        // SPOP with count on a missing key returns an empty array, not null_bulk.
        return Ok(if args.len() == 2 {
            RespValue::array(vec![])
        } else {
            RespValue::null_bulk()
        });
    }
    let count = if args.len() == 2 {
        std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))?
    } else {
        1
    };
    let with_count = args.len() == 2;
    let old_mem = key_mem(sh, &args[0]);
    let mut popped = Vec::new();
    let empty = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
        JxValue::Set(s) => {
            for _ in 0..count {
                if let Some(v) = s.iter().next().cloned() {
                    s.remove(&v);
                    popped.push(v);
                } else {
                    break;
                }
            }
            s.is_empty()
        }
        _ => return Err(wrongtype()),
    };
    if empty {
        drop_if_empty(sh, &args[0], old_mem);
    } else {
        resync_key_mem(sh, &args[0], old_mem);
    }
    if with_count {
        Ok(RespValue::array(
            popped.into_iter().map(RespValue::bulk).collect(),
        ))
    } else {
        Ok(popped
            .into_iter()
            .next()
            .map(RespValue::bulk)
            .unwrap_or(RespValue::null_bulk()))
    }
}

pub fn srandmember(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.is_empty() || args.len() > 2 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    // Validate count before touching the key so invalid integers always error.
    let count = if args.len() == 2 {
        Some(
            std::str::from_utf8(&args[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| RespValue::err("ERR value is not an integer"))?,
        )
    } else {
        None
    };
    if !sh.touch_alive(&args[0], now) {
        // Missing key: count mode returns empty array, no-count returns null_bulk.
        return Ok(match count {
            Some(_) => RespValue::array(vec![]),
            None => RespValue::null_bulk(),
        });
    }
    match &sh.dict[&args[0]].value {
        JxValue::Set(s) => match count {
            None => Ok(s
                .iter()
                .next()
                .map(|v| RespValue::bulk(v.clone()))
                .unwrap_or(RespValue::null_bulk())),
            Some(c) => {
                // Redis may return duplicates for negative counts; MVP returns up to
                // |c| distinct members without duplicates.
                let n = (c.unsigned_abs() as usize).min(s.len());
                let members: Vec<Bytes> = s.iter().cloned().collect();
                let picked: Vec<Bytes> = members
                    .choose_multiple(&mut rand::thread_rng(), n)
                    .cloned()
                    .collect();
                Ok(RespValue::array(
                    picked.into_iter().map(RespValue::bulk).collect(),
                ))
            }
        },
        _ => Err(wrongtype()),
    }
}
