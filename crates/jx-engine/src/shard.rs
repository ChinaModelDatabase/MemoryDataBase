use std::collections::HashMap;

use bytes::Bytes;
use jx_proto::RespValue;

use crate::types::{Entry, MaxMemoryPolicy};
use crate::{commands, entry_mem, evict, ttl};

pub struct ShardState {
    pub dict: HashMap<Bytes, Entry>,
    pub mem: usize,
    pub maxmemory: Option<usize>,
    pub policy: MaxMemoryPolicy,
}

impl ShardState {
    pub fn new_empty(policy: MaxMemoryPolicy) -> Self {
        Self {
            dict: HashMap::new(),
            mem: 0,
            maxmemory: None,
            policy,
        }
    }

    /// Mark `key` alive: bump last_access/freq; drop it (and mem charge) if expired.
    pub fn touch_alive(&mut self, key: &Bytes, now: u64) -> bool {
        if let Some(e) = self.dict.get(key) {
            if let Some(exp) = e.meta.expire_at_ms {
                if now >= exp {
                    let m = entry_mem(key, &e.value);
                    self.dict.remove(key);
                    self.mem = self.mem.saturating_sub(m);
                    crate::stats::inc(&crate::stats::STATS.expired_keys);
                    return false;
                }
            }
        }
        if let Some(e) = self.dict.get_mut(key) {
            e.meta.last_access_ms = now;
            e.meta.freq = e.meta.freq.saturating_add(1);
            true
        } else {
            false
        }
    }

    pub fn expire_cycle(&mut self, now: u64, budget: usize) -> u64 {
        ttl::expire_cycle(self, now, budget)
    }

    pub fn evict_if_needed(&mut self, key: &Bytes, now: u64) -> Result<(), RespValue> {
        evict::evict_if_needed(self, key, now)
    }

    pub fn execute(&mut self, args: &[Bytes], now: u64) -> Result<RespValue, RespValue> {
        commands::dispatch(self, args, now)
    }
}
