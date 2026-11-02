use std::collections::{HashMap, HashSet, VecDeque};

use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct ZSet {
    pub entries: HashMap<Bytes, f64>,
}

#[derive(Debug, Clone)]
pub enum JxValue {
    Str(Bytes),
    Int(i64),
    Hash(HashMap<Bytes, Bytes>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    ZSet(ZSet),
}

impl JxValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            JxValue::Str(_) | JxValue::Int(_) => "string",
            JxValue::Hash(_) => "hash",
            JxValue::List(_) => "list",
            JxValue::Set(_) => "set",
            JxValue::ZSet(_) => "zset",
        }
    }

    /// Approximate in-memory footprint, used for maxmemory accounting.
    pub fn approx_bytes(&self) -> usize {
        match self {
            JxValue::Str(b) => b.len() + 32,
            JxValue::Int(_) => 32,
            JxValue::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 48).sum::<usize>() + 32,
            JxValue::List(l) => l.iter().map(|x| x.len() + 24).sum::<usize>() + 32,
            JxValue::Set(s) => s.iter().map(|x| x.len() + 32).sum::<usize>() + 32,
            JxValue::ZSet(z) => z.entries.keys().map(|k| k.len() + 40).sum::<usize>() + 32,
        }
    }
}

/// Approximate in-memory footprint of one dict entry (key + value + overhead).
pub fn entry_mem(key: &Bytes, v: &JxValue) -> usize {
    key.len() + v.approx_bytes() + 32
}

#[derive(Debug, Clone, Default)]
pub struct KeyMeta {
    pub expire_at_ms: Option<u64>,
    pub last_access_ms: u64,
    pub freq: u64,
}

pub struct Entry {
    pub value: JxValue,
    pub meta: KeyMeta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxMemoryPolicy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysLfu,
    VolatileLfu,
    VolatileTtl,
}

impl MaxMemoryPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            MaxMemoryPolicy::NoEviction => "noeviction",
            MaxMemoryPolicy::AllKeysLru => "allkeys-lru",
            MaxMemoryPolicy::VolatileLru => "volatile-lru",
            MaxMemoryPolicy::AllKeysLfu => "allkeys-lfu",
            MaxMemoryPolicy::VolatileLfu => "volatile-lfu",
            MaxMemoryPolicy::VolatileTtl => "volatile-ttl",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "noeviction" => Some(MaxMemoryPolicy::NoEviction),
            "allkeys-lru" => Some(MaxMemoryPolicy::AllKeysLru),
            "volatile-lru" => Some(MaxMemoryPolicy::VolatileLru),
            "allkeys-lfu" => Some(MaxMemoryPolicy::AllKeysLfu),
            "volatile-lfu" => Some(MaxMemoryPolicy::VolatileLfu),
            "volatile-ttl" => Some(MaxMemoryPolicy::VolatileTtl),
            _ => None,
        }
    }
}
