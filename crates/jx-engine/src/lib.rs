pub mod actor;
pub mod commands;
pub mod coordinator;
pub mod evict;
pub mod shard;
pub mod stats;
pub mod ttl;
mod types;

use bytes::Bytes;

pub use shard::ShardState;
pub use types::*;

/// Approximate `entry_mem` for the current value at `key` (0 if missing).
pub fn key_mem(sh: &ShardState, key: &Bytes) -> usize {
    sh.dict
        .get(key)
        .map(|e| entry_mem(key, &e.value))
        .unwrap_or(0)
}

/// After in-place mutation of the value at `key`, adjust `shard.mem` from
/// `old_mem` (measured immediately before the mutation via `key_mem`) to the
/// current approximate size.
pub fn resync_key_mem(sh: &mut ShardState, key: &Bytes, old_mem: usize) {
    let new_mem = key_mem(sh, key);
    sh.mem = sh.mem.saturating_sub(old_mem);
    sh.mem = sh.mem.saturating_add(new_mem);
}

pub fn system_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
