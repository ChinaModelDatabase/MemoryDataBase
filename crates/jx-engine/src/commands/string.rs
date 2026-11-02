use bytes::Bytes;
use jx_proto::RespValue;

use crate::{
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

fn try_canonical_int(b: &Bytes) -> Option<i64> {
    if b.is_empty() || b.len() > 20 {
        return None;
    }
    let s = std::str::from_utf8(b).ok()?;
    let n: i64 = s.parse().ok()?;
    (n.to_string().as_bytes() == b.as_ref()).then_some(n)
}

fn put_str(sh: &mut ShardState, key: &Bytes, val: Bytes, now: u64, expire: Option<u64>) {
    if let Some(old) = sh.dict.remove(key) {
        sh.mem = sh.mem.saturating_sub(entry_mem(key, &old.value));
    }
    let value = match try_canonical_int(&val) {
        Some(n) => JxValue::Int(n),
        None => JxValue::Str(val),
    };
    sh.mem += entry_mem(key, &value);
    sh.dict.insert(
        key.clone(),
        Entry {
            value,
            meta: KeyMeta {
                expire_at_ms: expire,
                last_access_ms: now,
                freq: 1,
            },
        },
    );
}

pub fn set(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() < 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'set' command",
        ));
    }
    let key = &args[0];
    let val = args[1].clone();
    let mut expire: Option<u64> = None;
    let mut i = 2;
    while i < args.len() {
        let flag = std::str::from_utf8(&args[i])
            .unwrap_or("")
            .to_ascii_uppercase();
        match flag.as_str() {
            "EX" | "PX" => {
                let n = args
                    .get(i + 1)
                    .ok_or_else(|| RespValue::err("ERR syntax error"))?;
                let n = parse_i64(n)?.max(0) as u64;
                expire = Some(if flag == "EX" {
                    now.saturating_add(n.saturating_mul(1000))
                } else {
                    now.saturating_add(n)
                });
                i += 2;
            }
            _ => return Err(RespValue::err("ERR syntax error")),
        }
    }
    sh.evict_if_needed(key, now)?;
    if sh.touch_alive(key, now) && !matches!(sh.dict[key].value, JxValue::Str(_) | JxValue::Int(_))
    {
        return Err(wrongtype());
    }
    put_str(sh, key, val, now, expire);
    Ok(RespValue::ok())
}

pub fn get(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'get' command",
        ));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::null_bulk());
    }
    match &sh.dict[&args[0]].value {
        JxValue::Str(b) => Ok(RespValue::bulk(b.clone())),
        JxValue::Int(n) => Ok(RespValue::bulk(Bytes::from(n.to_string()))),
        _ => Err(wrongtype()),
    }
}

pub fn setnx(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'setnx' command",
        ));
    }
    sh.evict_if_needed(&args[0], now)?;
    if sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    put_str(sh, &args[0], args[1].clone(), now, None);
    Ok(RespValue::int(1))
}

/// SETEX key seconds value — SET with EX.
pub fn setex(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'setex' command",
        ));
    }
    set(
        sh,
        &[
            args[0].clone(),
            args[2].clone(),
            Bytes::from_static(b"EX"),
            args[1].clone(),
        ],
        now,
    )
}

/// PSETEX key milliseconds value — SET with PX.
pub fn psetex(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'psetex' command",
        ));
    }
    set(
        sh,
        &[
            args[0].clone(),
            args[2].clone(),
            Bytes::from_static(b"PX"),
            args[1].clone(),
        ],
        now,
    )
}

/// MSETNX key value [key value ...] — set all only if none exist (this shard).
pub fn msetnx(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'msetnx' command",
        ));
    }
    for pair in args.chunks(2) {
        if sh.touch_alive(&pair[0], now) {
            return Ok(RespValue::int(0));
        }
    }
    for pair in args.chunks(2) {
        set(sh, &[pair[0].clone(), pair[1].clone()], now)?;
    }
    Ok(RespValue::int(1))
}

fn read_int(sh: &ShardState, key: &Bytes) -> Result<i64, RespValue> {
    let e = sh
        .dict
        .get(key)
        .ok_or_else(|| RespValue::err("ERR no such key"))?;
    match &e.value {
        JxValue::Int(n) => Ok(*n),
        JxValue::Str(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RespValue::err("ERR value is not an integer or out of range")),
        _ => Err(wrongtype()),
    }
}

pub fn incr(
    sh: &mut ShardState,
    args: &[Bytes],
    now: u64,
    delta: i64,
) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err("ERR wrong number of arguments"));
    }
    apply_delta(sh, &args[0], delta, now)
}

pub fn incrby(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'incrby' command",
        ));
    }
    apply_delta(sh, &args[0], parse_i64(&args[1])?, now)
}

pub fn decrby(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'decrby' command",
        ));
    }
    let n = parse_i64(&args[1])?;
    // i64::MIN has no positive counterpart; reject instead of wrapping.
    let delta = n
        .checked_neg()
        .ok_or_else(|| RespValue::err("ERR increment or decrement would overflow"))?;
    apply_delta(sh, &args[0], delta, now)
}

fn apply_delta(
    sh: &mut ShardState,
    key: &Bytes,
    delta: i64,
    now: u64,
) -> Result<RespValue, RespValue> {
    sh.evict_if_needed(key, now)?;
    if !sh.touch_alive(key, now) {
        let value = JxValue::Int(delta);
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
        return Ok(RespValue::int(delta));
    }
    let cur = read_int(sh, key)?;
    let new = cur.saturating_add(delta);
    let old_mem = entry_mem(key, &sh.dict[key].value);
    sh.dict.get_mut(key).unwrap().value = JxValue::Int(new);
    let new_mem = entry_mem(key, &sh.dict[key].value);
    sh.mem = sh.mem.saturating_sub(old_mem);
    sh.mem += new_mem;
    Ok(RespValue::int(new))
}

pub fn append(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 2 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'append' command",
        ));
    }
    sh.evict_if_needed(&args[0], now)?;
    let mut buf = Vec::new();
    let mut expire: Option<u64> = None;
    if sh.touch_alive(&args[0], now) {
        match &sh.dict[&args[0]].value {
            JxValue::Str(b) => buf.extend_from_slice(b),
            JxValue::Int(n) => buf.extend_from_slice(n.to_string().as_bytes()),
            _ => return Err(wrongtype()),
        }
        // APPEND must preserve an existing TTL (Redis semantics).
        expire = sh.dict[&args[0]].meta.expire_at_ms;
    }
    buf.extend_from_slice(&args[1]);
    let len = buf.len() as i64;
    put_str(sh, &args[0], Bytes::from(buf), now, expire);
    Ok(RespValue::int(len))
}

pub fn strlen(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 1 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'strlen' command",
        ));
    }
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::int(0));
    }
    match &sh.dict[&args[0]].value {
        JxValue::Str(b) => Ok(RespValue::int(b.len() as i64)),
        JxValue::Int(n) => Ok(RespValue::int(n.to_string().len() as i64)),
        _ => Err(wrongtype()),
    }
}

pub fn getrange(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.len() != 3 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'getrange' command",
        ));
    }
    let start = parse_i64(&args[1])?;
    let stop = parse_i64(&args[2])?;
    if !sh.touch_alive(&args[0], now) {
        return Ok(RespValue::bulk(Bytes::new()));
    }
    let b = match &sh.dict[&args[0]].value {
        JxValue::Str(b) => b.clone(),
        JxValue::Int(n) => Bytes::from(n.to_string()),
        _ => return Err(wrongtype()),
    };
    let len = b.len() as i64;
    if len == 0 {
        return Ok(RespValue::bulk(Bytes::new()));
    }
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
        return Ok(RespValue::bulk(Bytes::new()));
    }
    Ok(RespValue::bulk(b.slice(s as usize..=t as usize)))
}

pub fn mget(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    let mut out = Vec::with_capacity(args.len());
    for k in args {
        // Propagate WRONGTYPE (and other per-key errors); null only for missing keys.
        out.push(get(sh, std::slice::from_ref(k), now)?);
    }
    Ok(RespValue::array(out))
}

pub fn mset(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(RespValue::err(
            "ERR wrong number of arguments for 'mset' command",
        ));
    }
    for pair in args.chunks(2) {
        set(sh, &[pair[0].clone(), pair[1].clone()], now)?;
    }
    Ok(RespValue::ok())
}
