use bytes::Bytes;
use jx_proto::RespValue;

use crate::{entry_mem, MaxMemoryPolicy, ShardState};

/// Shard-local eviction check. `maxmemory` is this shard's limit; `mem` is
/// this shard's used memory. limit==0 (or unset) means unlimited.
pub fn evict_if_needed(sh: &mut ShardState, key: &Bytes, _now: u64) -> Result<(), RespValue> {
    let Some(limit) = sh.maxmemory else {
        return Ok(());
    };
    // Redis: maxmemory 0 (or unset) means unlimited — skip eviction/OOM entirely.
    if limit == 0 || sh.mem < limit {
        return Ok(());
    }
    if matches!(sh.policy, MaxMemoryPolicy::NoEviction) {
        return Err(RespValue::err(
            "OOM command not allowed when used memory > 'maxmemory'",
        ));
    }
    // 采样 5 个 key（跳过正在写入的 key，避免误删破坏 SETNX/WRONGTYPE 语义），优先淘汰有过期时间的
    let mut candidates: Vec<(u64, Bytes, u64, Option<u64>)> = sh
        .dict
        .iter()
        .filter(|(k, _)| k.as_ref() != key.as_ref())
        .take(5)
        .map(|(k, ent)| {
            (
                ent.meta.freq,
                k.clone(),
                ent.meta.last_access_ms,
                ent.meta.expire_at_ms,
            )
        })
        .collect();
    let volatile_first = matches!(
        sh.policy,
        MaxMemoryPolicy::VolatileLru | MaxMemoryPolicy::VolatileLfu | MaxMemoryPolicy::VolatileTtl
    );
    candidates.sort_by(|a, b| {
        if volatile_first {
            let av = a.3.is_some() as u8;
            let bv = b.3.is_some() as u8;
            bv.cmp(&av).then(a.0.cmp(&b.0)).then(a.2.cmp(&b.2))
        } else {
            a.0.cmp(&b.0).then(a.2.cmp(&b.2))
        }
    });
    if let Some((_, victim, _, _)) = candidates.into_iter().next() {
        if let Some(old) = sh.dict.remove(&victim) {
            sh.mem = sh.mem.saturating_sub(entry_mem(&victim, &old.value));
            crate::stats::inc(&crate::stats::STATS.evicted_keys);
        }
        Ok(())
    } else {
        Err(RespValue::err(
            "OOM command not allowed when used memory > 'maxmemory'",
        ))
    }
}
