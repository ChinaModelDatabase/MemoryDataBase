use bytes::Bytes;

use crate::{entry_mem, ShardState};

/// Expire up to `budget` keys from this shard. Returns how many were removed.
pub fn expire_cycle(sh: &mut ShardState, now: u64, budget: usize) -> u64 {
    if budget == 0 {
        return 0;
    }
    let mut removed = 0u64;
    let expired: Vec<Bytes> = sh
        .dict
        .iter()
        .filter(|(_, ent)| ent.meta.expire_at_ms.map(|exp| now >= exp).unwrap_or(false))
        .map(|(k, _)| k.clone())
        .take(budget)
        .collect();
    for k in expired {
        if let Some(old) = sh.dict.remove(&k) {
            sh.mem = sh.mem.saturating_sub(entry_mem(&k, &old.value));
        }
        removed += 1;
    }
    crate::stats::add(&crate::stats::STATS.expired_keys, removed);
    removed
}
