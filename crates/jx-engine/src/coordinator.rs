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
