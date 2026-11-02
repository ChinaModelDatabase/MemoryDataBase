pub mod hash;
pub mod key;
pub mod list;
pub mod server;
pub mod set;
pub mod string;
pub mod zset;

use bytes::Bytes;
use jx_proto::RespValue;

use crate::ShardState;

/// 返回 true 表示已知但本版本未实现（后续任务逐步从列表移除）
pub fn is_unimplemented(cmd: &str) -> bool {
    matches!(
        cmd,
        "MULTI"
            | "EXEC"
            | "DISCARD"
            | "WATCH"
            | "SUBSCRIBE"
            | "PUBLISH"
            | "UNSUBSCRIBE"
            | "EVAL"
            | "EVALSHA"
            | "XADD"
            | "XRANGE"
            | "GEOADD"
            | "PFADD"
            | "HELLO"
            | "COMMAND"
            | "MONITOR"
            | "AUTH"
            | "GETBIT"
            | "SETBIT"
            | "BITCOUNT"
            | "HMSET"
    )
}

#[inline(always)]
fn cmd_eq(name: &[u8], upper: &str) -> bool {
    name.len() == upper.len()
        && name
            .iter()
            .zip(upper.bytes())
            .all(|(a, b)| a.to_ascii_uppercase() == b)
}

/// Route a command against a single `ShardState`.
///
/// Multi-shard commands (INFO/KEYS/FLUSHALL/CONFIG/MGET/...) operate on this
/// shard only; the Engine-level `execute` path aggregates across shards for
/// the shared `Engine` API until Task 2's coordinator takes over.
pub fn dispatch(sh: &mut ShardState, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
    if args.is_empty() {
        return Err(RespValue::err("ERR empty command"));
    }
    let name = args[0].as_ref();
    let a = &args[1..];
    if cmd_eq(name, "PING") {
        Ok(RespValue::SimpleString("PONG".into()))
    } else if cmd_eq(name, "ECHO") {
        if a.len() == 1 {
            Ok(RespValue::bulk(a[0].clone()))
        } else {
            Err(RespValue::err(
                "ERR wrong number of arguments for 'echo' command",
            ))
        }
    } else if cmd_eq(name, "QUIT") {
        Ok(RespValue::ok())
    } else if cmd_eq(name, "SET") {
        string::set(sh, a, now)
    } else if cmd_eq(name, "GET") {
        string::get(sh, a, now)
    } else if cmd_eq(name, "SETNX") {
        string::setnx(sh, a, now)
    } else if cmd_eq(name, "SETEX") {
        string::setex(sh, a, now)
    } else if cmd_eq(name, "PSETEX") {
        string::psetex(sh, a, now)
    } else if cmd_eq(name, "MSETNX") {
        string::msetnx(sh, a, now)
    } else if cmd_eq(name, "INCR") {
        string::incr(sh, a, now, 1)
    } else if cmd_eq(name, "DECR") {
        string::incr(sh, a, now, -1)
    } else if cmd_eq(name, "INCRBY") {
        string::incrby(sh, a, now)
    } else if cmd_eq(name, "DECRBY") {
        string::decrby(sh, a, now)
    } else if cmd_eq(name, "APPEND") {
        string::append(sh, a, now)
    } else if cmd_eq(name, "STRLEN") {
        string::strlen(sh, a, now)
    } else if cmd_eq(name, "GETRANGE") {
        string::getrange(sh, a, now)
    } else if cmd_eq(name, "MGET") {
        string::mget(sh, a, now)
    } else if cmd_eq(name, "MSET") {
        string::mset(sh, a, now)
    } else if cmd_eq(name, "DEL") {
        key::del(sh, a, now)
    } else if cmd_eq(name, "EXISTS") {
        key::exists(sh, a, now)
    } else if cmd_eq(name, "TYPE") {
        key::key_type(sh, a, now)
    } else if cmd_eq(name, "RENAME") {
        key::rename(sh, a, now)
    } else if cmd_eq(name, "RANDOMKEY") {
        key::randomkey(sh, a, now)
    } else if cmd_eq(name, "EXPIRE") {
        key::expire(sh, a, now, false)
    } else if cmd_eq(name, "PEXPIRE") {
        key::expire(sh, a, now, true)
    } else if cmd_eq(name, "EXPIREAT") {
        key::expire_at(sh, a, now, false)
    } else if cmd_eq(name, "PEXPIREAT") {
        key::expire_at(sh, a, now, true)
    } else if cmd_eq(name, "TTL") {
        key::ttl(sh, a, now, false)
    } else if cmd_eq(name, "PTTL") {
        key::ttl(sh, a, now, true)
    } else if cmd_eq(name, "PERSIST") {
        key::persist(sh, a, now)
    } else if cmd_eq(name, "HSET") {
        hash::hset(sh, a, now)
    } else if cmd_eq(name, "HGET") {
        hash::hget(sh, a, now)
    } else if cmd_eq(name, "HMGET") {
        hash::hmget(sh, a, now)
    } else if cmd_eq(name, "HDEL") {
        hash::hdel(sh, a, now)
    } else if cmd_eq(name, "HEXISTS") {
        hash::hexists(sh, a, now)
    } else if cmd_eq(name, "HLEN") {
        hash::hlen(sh, a, now)
    } else if cmd_eq(name, "HKEYS") {
        hash::hkeys(sh, a, now)
    } else if cmd_eq(name, "HVALS") {
        hash::hvals(sh, a, now)
    } else if cmd_eq(name, "HGETALL") {
        hash::hgetall(sh, a, now)
    } else if cmd_eq(name, "HINCRBY") {
        hash::hincrby(sh, a, now)
    } else if cmd_eq(name, "LPUSH") {
        list::lpush(sh, a, now)
    } else if cmd_eq(name, "RPUSH") {
        list::rpush(sh, a, now)
    } else if cmd_eq(name, "LPOP") {
        list::lpop(sh, a, now)
    } else if cmd_eq(name, "RPOP") {
        list::rpop(sh, a, now)
    } else if cmd_eq(name, "LRANGE") {
        list::lrange(sh, a, now)
    } else if cmd_eq(name, "LLEN") {
        list::llen(sh, a, now)
    } else if cmd_eq(name, "LINDEX") {
        list::lindex(sh, a, now)
    } else if cmd_eq(name, "LSET") {
        list::lset(sh, a, now)
    } else if cmd_eq(name, "SADD") {
        set::sadd(sh, a, now)
    } else if cmd_eq(name, "SREM") {
        set::srem(sh, a, now)
    } else if cmd_eq(name, "SMEMBERS") {
        set::smembers(sh, a, now)
    } else if cmd_eq(name, "SISMEMBER") {
        set::sismember(sh, a, now)
    } else if cmd_eq(name, "SCARD") {
        set::scard(sh, a, now)
    } else if cmd_eq(name, "SPOP") {
        set::spop(sh, a, now)
    } else if cmd_eq(name, "SRANDMEMBER") {
        set::srandmember(sh, a, now)
    } else if cmd_eq(name, "ZADD") {
        zset::zadd(sh, a, now)
    } else if cmd_eq(name, "ZREM") {
        zset::zrem(sh, a, now)
    } else if cmd_eq(name, "ZSCORE") {
        zset::zscore(sh, a, now)
    } else if cmd_eq(name, "ZCARD") {
        zset::zcard(sh, a, now)
    } else if cmd_eq(name, "ZRANGE") {
        zset::zrange(sh, a, now)
    } else if cmd_eq(name, "ZRANGEBYSCORE") {
        zset::zrangebyscore(sh, a, now)
    } else if cmd_eq(name, "ZINCRBY") {
        zset::zincrby(sh, a, now)
    } else if cmd_eq(name, "DBSIZE") {
        Ok(RespValue::int(sh.dict.len() as i64))
    } else if cmd_eq(name, "INFO") {
        server::info(sh, a, now)
    } else if cmd_eq(name, "FLUSHALL") || cmd_eq(name, "FLUSHDB") {
        server::flushall(sh, a, now)
    } else if cmd_eq(name, "KEYS") {
        server::keys(sh, a, now)
    } else if cmd_eq(name, "CONFIG") {
        server::config(sh, a, now)
    } else if cmd_eq(name, "TIME") {
        let ms = now;
        Ok(RespValue::array(vec![
            RespValue::bulk(Bytes::from((ms / 1000).to_string())),
            RespValue::bulk(Bytes::from((ms % 1000 * 1000).to_string())),
        ]))
    } else if cmd_eq(name, "SELECT") {
        if a.len() != 1 {
            return Err(RespValue::err(
                "ERR wrong number of arguments for 'select' command",
            ));
        }
        let db = std::str::from_utf8(&a[0])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| RespValue::err("ERR invalid DB index"))?;
        if db == 0 {
            Ok(RespValue::ok())
        } else {
            Err(RespValue::err("ERR DB index is out of range"))
        }
    } else {
        let name = std::str::from_utf8(&args[0])
            .unwrap_or("")
            .to_ascii_uppercase();
        if is_unimplemented(&name) {
            Err(RespValue::err(format!(
                "ERR command '{name}' is not supported in this version"
            )))
        } else {
            Err(RespValue::err(format!("ERR unknown command '{name}'")))
        }
    }
}
