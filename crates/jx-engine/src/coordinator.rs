//! Cross-shard coordinator: public APIs for multi-shard commands that
//! `ShardRuntime::exec` and the server conn layer both delegate to.
//!
//! All functions operate through the shard actor channels (`Cmd`), never by
//! reaching into another thread's `ShardState` directly.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use jx_proto::RespValue;

use crate::actor::ShardRuntime;

/// Cross-shard MGET. Fan-out is parallel (one scoped thread per shard group);
/// results are remapped back into **input key order**. Missing keys are
/// null-bulk. Errors from any shard (e.g. `ERR shard command queue full`,
/// WRONGTYPE) abort the whole command.
pub fn mget_coalesced(rt: &ShardRuntime, keys: &[Bytes]) -> RespValue {
    if keys.is_empty() {
        return RespValue::array(vec![]);
    }
    let mut groups: Vec<Vec<usize>> = vec![Vec::new(); rt.shard_count()];
    for (i, k) in keys.iter().enumerate() {
        groups[rt.shard_id_for_key(k)].push(i);
    }
    // One result slot per input key (order remap).
    let mut slots: Vec<Option<RespValue>> = vec![None; keys.len()];
    let mut error: Option<RespValue> = None;
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(groups.len());
        for (sid, idxs) in groups.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let sub_keys: Vec<Bytes> = idxs.iter().map(|&i| keys[i].clone()).collect();
            handles.push(scope.spawn(move || {
                let mut sub = Vec::with_capacity(sub_keys.len() + 1);
                sub.push(Bytes::from_static(b"MGET"));
                sub.extend(sub_keys);
                (idxs, rt.try_exec_on(sid, sub, false))
            }));
        }
        for h in handles {
            let (idxs, resp) = match h.join() {
                Ok(v) => v,
                Err(_) => {
                    if error.is_none() {
                        error = Some(RespValue::err("ERR shard actor crashed"));
                    }
                    continue;
                }
            };
            if error.is_some() {
                continue;
            }
            match resp {
                RespValue::Array(Some(items)) if items.len() == idxs.len() => {
                    for (slot, item) in idxs.into_iter().zip(items) {
                        slots[slot] = Some(item);
                    }
                }
                RespValue::Error(m) => error = Some(RespValue::err(m)),
                other => error = Some(other),
            }
        }
    });
    if let Some(e) = error {
        return e;
    }
    RespValue::array(
        slots
            .into_iter()
            .map(|x| x.expect("every key slot filled"))
            .collect(),
    )
}

/// Cross-shard KEYS with a glob pattern. Broadcasts to every shard, dedupes
/// nothing beyond shard-local collection (keys hash to exactly one shard),
/// returns an array of bulk strings.
pub fn keys_glob(rt: &ShardRuntime, pattern: &Bytes) -> RespValue {
    let pat = String::from_utf8_lossy(pattern).to_string();
    let now = rt.now();
    let parts: Arc<Mutex<Vec<Vec<Bytes>>>> =
        Arc::new(Mutex::new(vec![Vec::new(); rt.shard_count()]));
    let p2 = parts.clone();
    rt.broadcast(move |id, sh| {
        let ks = crate::commands::server::collect_keys(sh, &pat, now);
        p2.lock().unwrap()[id] = ks;
    });
    let mut out = Vec::new();
    for ks in parts.lock().unwrap().iter() {
        out.extend(ks.iter().cloned());
    }
    RespValue::array(out.into_iter().map(RespValue::bulk).collect())
}

/// Cross-shard FLUSHALL/FLUSHDB: clear every shard.
pub fn flushall(rt: &ShardRuntime) -> RespValue {
    rt.flush_all_shards();
    RespValue::ok()
}

/// Cross-shard DBSIZE: sum of live keys across all shards.
pub fn dbsize(rt: &ShardRuntime) -> RespValue {
    RespValue::int(rt.stats().0 as i64)
}

/// Aggregate maxmemory across shards plus the effective eviction policy
/// (`INFO memory`: `maxmemory` / `maxmemory_policy`).
pub fn memory_limits(rt: &ShardRuntime) -> (usize, String) {
    let slots: Arc<Mutex<Vec<(usize, String)>>> =
        Arc::new(Mutex::new(vec![(0, String::new()); rt.shard_count()]));
    let p2 = slots.clone();
    rt.broadcast(move |id, sh| {
        p2.lock().unwrap()[id] = (sh.maxmemory.unwrap_or(0), sh.policy.as_str().to_string());
    });
    let guard = slots.lock().unwrap();
    let total = guard.iter().map(|(m, _)| *m).sum::<usize>();
    let policy = guard
        .first()
        .map(|(_, p)| p.clone())
        .unwrap_or_else(|| "noeviction".to_string());
    (total, policy)
}

/// Cross-shard SCAN: cursor iteration over the whole keyspace.
///
/// `cursor` indexes the sorted union of every shard's matching keys; the
/// reply is `[next_cursor_bulk, [key...]]` and terminates with cursor `0`.
/// Keys added mid-iteration may be missed and keys may repeat (Redis allows
/// both), but no key present for the whole iteration is skipped.
pub fn scan(rt: &ShardRuntime, cursor: u64, pattern: &str, count: usize) -> RespValue {
    let now = rt.now();
    let parts: Arc<Mutex<Vec<Vec<Bytes>>>> =
        Arc::new(Mutex::new(vec![Vec::new(); rt.shard_count()]));
    let p2 = parts.clone();
    let pat = pattern.to_string();
    rt.broadcast(move |id, sh| {
        let mut ks = crate::commands::server::collect_keys(sh, &pat, now);
        ks.sort();
        p2.lock().unwrap()[id] = ks;
    });
    let mut all = Vec::new();
    for ks in parts.lock().unwrap().iter() {
        all.extend(ks.iter().cloned());
    }
    all.sort();
    let start = (cursor as usize).min(all.len());
    let end = (start + count).min(all.len());
    let keys: Vec<RespValue> = all[start..end]
        .iter()
        .cloned()
        .map(RespValue::bulk)
        .collect();
    let next = if end >= all.len() { 0 } else { end as u64 };
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(next.to_string())),
        RespValue::array(keys),
    ])
}

/// Cross-shard INFO: aggregated keys/expires/mem across all shards.
pub fn info(rt: &ShardRuntime) -> RespValue {
    let (keys, expires, mem) = rt.stats();
    let RespValue::BulkString(Some(text)) = crate::commands::server::info_text(keys, expires, mem)
    else {
        return RespValue::err("ERR info build failed");
    };
    let mut full = text.to_vec();
    full.extend_from_slice(crate::stats::info_stats_section().as_bytes());
    RespValue::bulk(bytes::Bytes::from(full))
}
