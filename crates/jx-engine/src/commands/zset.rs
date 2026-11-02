use std::cmp::Ordering;
use std::collections::HashMap;

use bytes::Bytes;
use jx_proto::RespValue;

use crate::{
    key_mem, resync_key_mem,
    types::{entry_mem, Entry, JxValue, KeyMeta, ZSet},
    ShardState,
};

fn wrongtype() -> RespValue {
    RespValue::err("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn parse_f64(b: &Bytes) -> Result<f64, RespValue> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not a valid float"))
}

fn parse_i64(b: &Bytes) -> Result<i64, RespValue> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range"))
}

fn fmt_f64(f: f64) -> String {
    if f == f.trunc() && f.is_finite() {
        format!("{}", f as i64)
    } else {
        format!("{f}")
    }
}

fn ensure_zset(sh: &mut ShardState, key: &Bytes, now: u64) -> Result<(), RespValue> {
    if sh.touch_alive(key, now) {
        match &sh.dict[key].value {
            JxValue::ZSet(_) => Ok(()),
            _ => Err(wrongtype()),
        }
    } else {
        let value = JxValue::ZSet(ZSet::default());
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

fn sorted_members(entries: &HashMap<Bytes, f64>) -> Vec<(Bytes, f64)> {
    let mut items: Vec<(Bytes, f64)> = entries.iter().map(|(k, s)| (k.clone(), *s)).collect();
    items.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });
    items
}

fn drop_if_empty(sh: &mut ShardState, key: &Bytes, old_mem: usize) {
    if sh
        .dict
        .get(key)
        .map(|e| matches!(e.value, JxValue::ZSet(ref z) if z.entries.is_empty()))
        .unwrap_or(false)
    {
        sh.mem = sh.mem.saturating_sub(old_mem);
        sh.dict.remove(key);
    }
}

pub fn zadd(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'zadd' command",
        ));
    }
    // Parse every score before mutating so a bad pair leaves dict/mem unchanged
    // (no partial insert, no mem drift). Mirrors zincrby parse-before-ensure.
    let pairs: Vec<(Bytes, f64)> = args[1..]
        .chunks(2)
        .map(|pair| Ok((pair[1].clone(), parse_f64(&pair[0])?)))
        .collect::<Result<_, RespValue>>()?;
    sh.evict_if_needed(&args[0], now)?;
    ensure_zset(sh, &args[0], now)?;
    let old_mem = key_mem(sh, &args[0]);
    let mut added = 0;
    {
        let z = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
            JxValue::ZSet(z) => z,
            _ => unreachable!(),
        };
        for (member, score) in pairs {
            if z.entries.insert(member, score).is_none() {
                added += 1;
            }
        }
    }
    resync_key_mem(sh, &args[0], old_mem);
    Ok(RespValue::int(added))
}

pub fn zrem(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'zrem' command",
        ));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    let old_mem = key_mem(sh, &args[0]);
    let mut n = 0;
    let empty = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
        JxValue::ZSet(z) => {
            for m in &args[1..] {
                if z.entries.remove(m).is_some() {
                    n += 1;
                }
            }
            z.entries.is_empty()
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

pub fn zscore(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::null_bulk());
    }
    match &sh.dict[&args[0]].value {
        JxValue::ZSet(z) => Ok(match z.entries.get(&args[1]) {
            Some(s) => RespValue::bulk(Bytes::from(fmt_f64(*s))),
            None => RespValue::null_bulk(),
        }),
        _ => Err(wrongtype()),
    }
}

pub fn zcard(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    match &sh.dict[&args[0]].value {
        JxValue::ZSet(z) => Ok(RespValue::int(z.entries.len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn zrange(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'zrange' command",
        ));
    }
    let start = parse_i64(&args[1])?;
    let stop = parse_i64(&args[2])?;
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::array(vec![]));
    }
    match &sh.dict[&args[0]].value {
        JxValue::ZSet(z) => {
            let items = sorted_members(&z.entries);
            let len = items.len() as i64;
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
                items
                    .into_iter()
                    .skip(s as usize)
                    .take((t - s + 1) as usize)
                    .map(|(m, _)| RespValue::bulk(m))
                    .collect(),
            ))
        }
        _ => Err(wrongtype()),
    }
}

fn parse_score_bound(b: &Bytes) -> Result<Option<f64>, RespValue> {
    let s = std::str::from_utf8(b).map_err(|_| RespValue::err("ERR min or max is not a float"))?;
    if s == "-inf" || s == "+inf" {
        return Ok(None);
    }
    s.parse::<f64>()
        .map(Some)
        .map_err(|_| RespValue::err("ERR min or max is not a float"))
}

pub fn zrangebyscore(
    sh: &mut ShardState,
    args: &[Bytes],
    now: u64,
) -> Result<RespValue, RespValue> {
    if args.len() != 4 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'zrangebyscore' command",
        ));
    }
    let min = parse_score_bound(&args[1])?;
    let max = parse_score_bound(&args[2])?;
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::array(vec![]));
    }
    match &sh.dict[&args[0]].value {
        JxValue::ZSet(z) => {
            let items: Vec<(Bytes, f64)> = sorted_members(&z.entries)
                .into_iter()
                .filter(|(_, s)| min.map(|lo| *s >= lo).unwrap_or(true))
                .filter(|(_, s)| max.map(|hi| *s <= hi).unwrap_or(true))
                .collect();
            Ok(RespValue::array(
                items.into_iter().map(|(m, _)| RespValue::bulk(m)).collect(),
            ))
        }
        _ => Err(wrongtype()),
    }
}

pub fn zincrby(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'zincrby' command",
        ));
    }
    let delta = parse_f64(&args[1])?;
    sh.evict_if_needed(&args[0], now)?;
    ensure_zset(sh, &args[0], now)?;
    let old_mem = key_mem(sh, &args[0]);
    let new;
    {
        let z = match &mut sh.dict.get_mut(&args[0]).unwrap().value {
            JxValue::ZSet(z) => z,
            _ => unreachable!(),
        };
        let cur = z.entries.get(&args[2]).copied().unwrap_or(0.0);
        new = cur + delta;
        z.entries.insert(args[2].clone(), new);
    }
    resync_key_mem(sh, &args[0], old_mem);
    Ok(RespValue::bulk(Bytes::from(fmt_f64(new))))
}
