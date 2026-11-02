//! Exclusive-ownership shard runtime: each shard thread owns its `ShardState`
//! exclusively (moved into the thread). There is no Engine-internal Mutex.
//!
//! Channel capacity: each shard worker uses a **bounded** `crossbeam_channel`
//! (capacity `N`, typically `maxclients` or 1024). When a shard's queue is
//! full, `ShardRuntime::exec` returns a RESP error (`ERR shard command queue
//! full`) instead of blocking indefinitely — this bounds actor-thread memory
//! under overload. Callers that prefer blocking can treat the error as retryable.
//!
//! Multi-shard commands (INFO/KEYS/FLUSHALL/MGET/DBSIZE) delegate to
//! `crate::coordinator`; CONFIG/RENAME/DEL/EXISTS/MSET/MSETNX remain here.
//!
//! Live AOF: single-shard `Cmd::Exec` writes append on the owning actor;
//! coordinator paths (DEL via DelKey, FLUSHALL, cross-shard RENAME, MSET
//! subsets) append via [`ShardRuntime::append_aof`] / `append_aof_all` so a
//! crash cannot silently drop deletes/flushes from the command log.

use std::sync::Arc;
use std::thread::JoinHandle;

use bytes::Bytes;
use crossbeam_channel::{Sender, TrySendError};
use jx_proto::RespValue;
use rand::seq::SliceRandom;

use crate::types::{entry_mem, Entry, JxValue, KeyMeta, MaxMemoryPolicy};
use crate::ShardState;

pub type Reply<T> = std::sync::mpsc::Sender<T>;

/// Actor-owned AOF append sink. The engine stays free of a `jx-persist`
/// dependency: the server attaches a sink implementation at runtime. Called
/// on the owning shard thread after a successful write command (no extra
/// channel hop). See `ShardRuntime::set_aof_sink`.
pub trait AofSink: Send + Sync {
    fn append(&self, sid: usize, args: &[Bytes]);
}

/// Shared optional sink: attached after recovery, detached freely.
type SharedAof = Arc<std::sync::RwLock<Option<Arc<dyn AofSink>>>>;

/// Shared closure shape for `Cmd::Broadcast`.
pub type BroadcastFn = Arc<dyn Fn(usize, &mut ShardState) + Send + Sync>;

pub enum Cmd {
    Exec {
        args: Vec<Bytes>,
        reply: Reply<Result<RespValue, RespValue>>,
        /// When true and the command is a successful write, the owning actor
        /// appends `args` to the attached `AofSink`. `exec_no_aof` sets this
        /// false so recovery replay does not re-append loaded commands.
        aof: bool,
    },
    ConfigMaxMemory {
        bytes: usize,
        policy: MaxMemoryPolicy,
        reply: Reply<()>,
    },
    ConfigPolicy {
        policy: MaxMemoryPolicy,
        reply: Reply<()>,
    },
    Snapshot {
        reply: Reply<Vec<(Bytes, JxValue, KeyMeta)>>,
    },
    LoadShard {
        entries: Vec<(Bytes, JxValue, KeyMeta)>,
        reply: Reply<()>,
    },
    /// Query shard 0's (maxmemory, policy). Used by CONFIG GET / runtime reads.
    MaxMemory {
        reply: Reply<(Option<usize>, MaxMemoryPolicy)>,
    },
    /// Expire up to `budget` keys on this shard; reply is count removed.
    Expire {
        now: u64,
        budget: usize,
        reply: Reply<u64>,
    },
    /// Run `f(shard_id, &mut ShardState)` on the owning thread; wait for ack.
    Broadcast { f: BroadcastFn, reply: Reply<()> },
    /// Cross-shard RENAME helper: touch_alive + remove entry from this shard.
    FetchRemove {
        key: Bytes,
        now: u64,
        reply: Reply<Option<(JxValue, KeyMeta)>>,
    },
    /// Insert (key, value, meta) with mem accounting.
    Insert {
        key: Bytes,
        value: JxValue,
        meta: KeyMeta,
        reply: Reply<()>,
    },
    /// DEL a single key (touch_alive + remove + mem); reply is 0/1.
    DelKey {
        key: Bytes,
        now: u64,
        reply: Reply<i64>,
    },
}

pub struct ShardHandle {
    pub id: usize,
    #[allow(dead_code)]
    pub join: JoinHandle<()>,
    pub tx: Sender<Cmd>,
}

pub struct ShardRuntime {
    pub shard_count: usize,
    pub shards: Vec<ShardHandle>,
    pub now_ms: fn() -> u64,
    aof: SharedAof,
}

/// True when `args[0]` names a state-mutating command that AOF must persist
/// on the **owning shard actor** via `Cmd::Exec`. Reads (GET/EXISTS/...) and
/// control commands are excluded.
///
/// DEL/FLUSHALL/FLUSHDB never route through `Cmd::Exec` (DelKey / coordinator
/// paths) — they append via `ShardRuntime::append_aof` instead. RENAME stays
/// here because the **same-shard** case executes as `Cmd::Exec`.
fn is_write_cmd(args: &[Bytes]) -> bool {
    let Some(name) = args.first() else {
        return false;
    };
    let n = std::str::from_utf8(name).unwrap_or("").to_ascii_uppercase();
    matches!(
        n.as_str(),
        // string
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "MSET" | "MSETNX" | "APPEND"
            | "INCR" | "DECR" | "INCRBY" | "DECRBY"
            // key (RENAME same-shard only; cross-shard appends explicitly)
            | "RENAME" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST"
            // hash
            | "HSET" | "HDEL" | "HINCRBY"
            // list
            | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LSET"
            // set
            | "SADD" | "SREM" | "SPOP"
            // zset
            | "ZADD" | "ZREM" | "ZINCRBY"
    )
}

/// Spawn shard workers with the default bounded channel capacity (1024).
pub fn spawn_shard_runtime(
    shard_count: usize,
    capacity: usize,
    now_ms: fn() -> u64,
) -> ShardRuntime {
    spawn_shard_runtime_with_capacity(shard_count, capacity, now_ms)
}

/// Spawn shard workers with a bounded per-shard command queue of `capacity`
/// (clamped to at least 1). See module docs for overload behavior.
pub fn spawn_shard_runtime_with_capacity(
    shard_count: usize,
    capacity: usize,
    now_ms: fn() -> u64,
) -> ShardRuntime {
    let shard_count = shard_count.max(1);
    let capacity = capacity.max(1);
    let aof: SharedAof = Arc::new(std::sync::RwLock::new(None));
    let mut shards = Vec::new();
    for id in 0..shard_count {
        let (tx, rx) = crossbeam_channel::bounded::<Cmd>(capacity);
        let aof_thread = aof.clone();
        let join = std::thread::spawn(move || {
            let mut sh = ShardState::new_empty(MaxMemoryPolicy::NoEviction);
            while let Ok(cmd) = rx.recv() {
                crate::stats::set_max(&crate::stats::STATS.shard_queue_depth_max, rx.len() as u64);
                match cmd {
                    Cmd::Exec {
                        args,
                        reply,
                        aof: aof_on,
                    } => {
                        let now = now_ms();
                        let res = sh.execute(&args, now);
                        if aof_on && res.is_ok() && is_write_cmd(&args) {
                            if let Ok(guard) = aof_thread.read() {
                                if let Some(sink) = guard.as_ref() {
                                    sink.append(id, &args);
                                }
                            }
                        }
                        let _ = reply.send(res);
                    }
                    Cmd::ConfigMaxMemory {
                        bytes,
                        policy,
                        reply,
                    } => {
                        sh.maxmemory = if bytes == 0 { None } else { Some(bytes) };
                        sh.policy = policy;
                        let _ = reply.send(());
                    }
                    Cmd::ConfigPolicy { policy, reply } => {
                        sh.policy = policy;
                        let _ = reply.send(());
                    }
                    Cmd::Snapshot { reply } => {
                        let _ = reply.send(
                            sh.dict
                                .iter()
                                .map(|(k, e)| (k.clone(), e.value.clone(), e.meta.clone()))
                                .collect(),
                        );
                    }
                    Cmd::LoadShard { entries, reply } => {
                        for (k, v, m) in entries {
                            let new_mem = entry_mem(&k, &v);
                            if let Some(old) = sh.dict.get(&k) {
                                sh.mem = sh.mem.saturating_sub(entry_mem(&k, &old.value));
                            }
                            sh.mem = sh.mem.saturating_add(new_mem);
                            sh.dict.insert(k, Entry { value: v, meta: m });
                        }
                        let _ = reply.send(());
                    }
                    Cmd::MaxMemory { reply } => {
                        let _ = reply.send((sh.maxmemory, sh.policy));
                    }
                    Cmd::Expire { now, budget, reply } => {
                        let _ = reply.send(crate::ttl::expire_cycle(&mut sh, now, budget));
                    }
                    Cmd::Broadcast { f, reply } => {
                        f(id, &mut sh);
                        let _ = reply.send(());
                    }
                    Cmd::FetchRemove { key, now, reply } => {
                        let got = if sh.touch_alive(&key, now) {
                            sh.dict.remove(&key).map(|e| {
                                sh.mem = sh.mem.saturating_sub(entry_mem(&key, &e.value));
                                (e.value, e.meta)
                            })
                        } else {
                            None
                        };
                        let _ = reply.send(got);
                    }
                    Cmd::Insert {
                        key,
                        value,
                        meta,
                        reply,
                    } => {
                        let new_mem = entry_mem(&key, &value);
                        if let Some(old) = sh.dict.get(&key) {
                            sh.mem = sh.mem.saturating_sub(entry_mem(&key, &old.value));
                        }
                        sh.mem = sh.mem.saturating_add(new_mem);
                        sh.dict.insert(key, Entry { value, meta });
                        let _ = reply.send(());
                    }
                    Cmd::DelKey { key, now, reply } => {
                        let n = if sh.touch_alive(&key, now) || sh.dict.contains_key(&key) {
                            if let Some(old) = sh.dict.remove(&key) {
                                sh.mem = sh.mem.saturating_sub(entry_mem(&key, &old.value));
                            }
                            1
                        } else {
                            0
                        };
                        let _ = reply.send(n);
                    }
                }
            }
        });
        shards.push(ShardHandle { id, join, tx });
    }
    ShardRuntime {
        shard_count,
        shards,
        now_ms,
        aof,
    }
}

impl ShardRuntime {
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    pub fn shard_id_for_key(&self, key: &Bytes) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % self.shard_count
    }

    pub fn now(&self) -> u64 {
        (self.now_ms)()
    }

    /// Attach (or detach with `None`) the actor-owned AOF sink. Call after
    /// recovery replay so replayed commands are not re-appended; the server
    /// only attaches for plaintext `appendonly yes` (encrypted durability is
    /// RDB + `rewrite_aof` full-stream seal — live per-shard append is an
    /// unsupported MVP limit, see jx-persist `FileAofSink`).
    pub fn set_aof_sink(&self, sink: Option<Arc<dyn AofSink>>) {
        *self.aof.write().unwrap_or_else(|p| p.into_inner()) = sink;
    }

    /// Append `args` to shard `sid`'s AOF sink, if one is attached. Used by
    /// coordinator paths (DEL/FLUSHALL/RENAME/MSET) that bypass `Cmd::Exec`.
    pub fn append_aof(&self, sid: usize, args: &[Bytes]) {
        if args.is_empty() {
            return;
        }
        if let Ok(guard) = self.aof.read() {
            if let Some(sink) = guard.as_ref() {
                sink.append(sid, args);
            }
        }
    }

    /// Append `args` to every shard's AOF sink (FLUSHALL/FLUSHDB).
    pub fn append_aof_all(&self, args: &[Bytes]) {
        for sid in 0..self.shard_count {
            self.append_aof(sid, args);
        }
    }

    /// Clear every shard (live FLUSHALL/FLUSHDB via coordinator).
    pub fn flush_all_shards(&self) {
        self.broadcast(|_, sh| {
            sh.dict.clear();
            sh.mem = 0;
        });
    }

    /// Clear only shard `sid`. Used during per-shard AOF replay so a FLUSHALL
    /// record in one shard's file cannot wipe post-flush writes recorded in a
    /// lower-sid file replayed earlier (each shard file logs FLUSHALL).
    pub fn flush_shard_local(&self, sid: usize) {
        let (tx, rx) = std::sync::mpsc::channel();
        if self.shards[sid]
            .tx
            .send(Cmd::Broadcast {
                f: Arc::new(|_, sh| {
                    sh.dict.clear();
                    sh.mem = 0;
                }),
                reply: tx,
            })
            .is_ok()
        {
            let _ = rx.recv();
        }
    }

    /// Hot path: route `args` to the owning shard via try_send. On a full
    /// bounded channel this returns `ERR shard command queue full` (does not
    /// block). Multi-shard commands delegate to `crate::coordinator` (single
    /// implementation shared with the conn layer).
    pub fn exec(&self, args: Vec<Bytes>) -> RespValue {
        self.exec_inner(args, true)
    }

    /// Like [`exec`](Self::exec) but never appends to the AOF sink. Used by
    /// AOF/RDB recovery replay so loaded commands are not written back out.
    pub fn exec_no_aof(&self, args: Vec<Bytes>) -> RespValue {
        self.exec_inner(args, false)
    }

    fn exec_inner(&self, args: Vec<Bytes>, aof: bool) -> RespValue {
        if args.is_empty() {
            return RespValue::err("ERR empty command");
        }
        let name = std::str::from_utf8(&args[0])
            .unwrap_or("")
            .to_ascii_uppercase();
        let a = &args[1..];
        match name.as_str() {
            "INFO" => crate::coordinator::info(self),
            "FLUSHALL" | "FLUSHDB" => {
                // Coordinator clears every shard. Log first so recovery can
                // replay the flush even if a later write's append is lost.
                if aof {
                    self.append_aof_all(&[Bytes::copy_from_slice(name.as_bytes())]);
                }
                crate::coordinator::flushall(self)
            }
            "KEYS" => {
                let pat = a
                    .first()
                    .cloned()
                    .unwrap_or_else(|| Bytes::from_static(b"*"));
                crate::coordinator::keys_glob(self, &pat)
            }
            "CONFIG" => self.config_cmd(a),
            "DBSIZE" => crate::coordinator::dbsize(self),
            "RANDOMKEY" => {
                let now = self.now();
                let candidates: Arc<std::sync::Mutex<Vec<Bytes>>> =
                    Arc::new(std::sync::Mutex::new(Vec::new()));
                let c2 = candidates.clone();
                self.broadcast(move |_id, sh| {
                    let keys: Vec<Bytes> = sh.dict.keys().cloned().collect();
                    for k in keys {
                        if sh.touch_alive(&k, now) {
                            c2.lock().unwrap().push(k);
                        }
                    }
                });
                let cands = candidates.lock().unwrap();
                if cands.is_empty() {
                    RespValue::null_bulk()
                } else {
                    let k = cands
                        .choose(&mut rand::thread_rng())
                        .expect("candidates non-empty")
                        .clone();
                    RespValue::bulk(k)
                }
            }
            "RENAME" => self.rename_cmd(a, aof),
            "DEL" => self.del_cmd(a, aof),
            "EXISTS" => self.exists_cmd(a),
            "MGET" => crate::coordinator::mget_coalesced(self, a),
            "MSET" => self.mset_cmd(a, aof),
            "MSETNX" => self.msetnx_cmd(a, aof),
            // Single-shard / keyless: route by first key arg.
            _ => {
                let key = args.get(1).cloned().unwrap_or_default();
                let sid = self.shard_id_for_key(&key);
                self.try_exec_on(sid, args, aof)
            }
        }
    }

    /// Set maxmemory and policy on every shard. `bytes == 0` means unlimited
    /// (stored as None), matching Redis CONFIG SET maxmemory 0. Control-plane:
    /// blocking send is acceptable, but failures are surfaced (not swallowed).
    pub fn set_maxmemory(&self, bytes: usize, policy: MaxMemoryPolicy) -> Result<(), RespValue> {
        for s in &self.shards {
            let (tx, rx) = std::sync::mpsc::channel();
            if let Err(e) = s.tx.send(Cmd::ConfigMaxMemory {
                bytes,
                policy,
                reply: tx,
            }) {
                tracing::error!(shard = s.id, error = %e, "set_maxmemory send failed");
                return Err(RespValue::err("ERR shard actor crashed"));
            }
            if rx.recv().is_err() {
                tracing::error!(shard = s.id, "set_maxmemory actor disconnected");
                return Err(RespValue::err("ERR shard actor crashed"));
            }
        }
        Ok(())
    }

    /// Update only the eviction policy on every shard; maxmemory untouched.
    pub fn set_maxmemory_policy(&self, policy: MaxMemoryPolicy) -> Result<(), RespValue> {
        for s in &self.shards {
            let (tx, rx) = std::sync::mpsc::channel();
            if let Err(e) = s.tx.send(Cmd::ConfigPolicy { policy, reply: tx }) {
                tracing::error!(shard = s.id, error = %e, "set_maxmemory_policy send failed");
                return Err(RespValue::err("ERR shard actor crashed"));
            }
            if rx.recv().is_err() {
                tracing::error!(shard = s.id, "set_maxmemory_policy actor disconnected");
                return Err(RespValue::err("ERR shard actor crashed"));
            }
        }
        Ok(())
    }

    pub fn maxmemory(&self) -> Option<usize> {
        self.query_config().and_then(|(m, _)| m)
    }

    pub fn maxmemory_policy(&self) -> MaxMemoryPolicy {
        self.query_config()
            .map(|(_, p)| p)
            .unwrap_or(MaxMemoryPolicy::NoEviction)
    }

    /// Run `f(shard_id, &mut ShardState)` on every owning thread; blocks until
    /// all shards ack so subsequent commands observe the effects.
    pub fn broadcast(&self, f: impl Fn(usize, &mut ShardState) + Send + Sync + 'static) {
        let f = Arc::new(f);
        for s in &self.shards {
            let (tx, rx) = std::sync::mpsc::channel();
            if let Err(e) = s.tx.send(Cmd::Broadcast {
                f: f.clone(),
                reply: tx,
            }) {
                tracing::error!(shard = s.id, error = %e, "broadcast send failed");
                continue;
            }
            if rx.recv().is_err() {
                tracing::error!(shard = s.id, "broadcast actor disconnected");
            }
        }
    }

    /// Snapshot one shard's dict entries.
    pub fn snapshot_shard(&self, sid: usize) -> Vec<(Bytes, JxValue, KeyMeta)> {
        let (tx, rx) = std::sync::mpsc::channel();
        if self.shards[sid]
            .tx
            .send(Cmd::Snapshot { reply: tx })
            .is_err()
        {
            return Vec::new();
        }
        rx.recv().unwrap_or_default()
    }

    /// Load entries into one shard (with mem accounting).
    pub fn load_shard(&self, sid: usize, entries: Vec<(Bytes, JxValue, KeyMeta)>) {
        let (tx, rx) = std::sync::mpsc::channel();
        if self.shards[sid]
            .tx
            .send(Cmd::LoadShard { entries, reply: tx })
            .is_ok()
        {
            let _ = rx.recv();
        }
    }

    /// Expire across all shards, honoring a global budget.
    pub fn expire_cycle(&self, now: u64, budget: usize) -> u64 {
        let mut removed = 0u64;
        for s in &self.shards {
            if removed as usize >= budget {
                break;
            }
            let (tx, rx) = std::sync::mpsc::channel();
            if s.tx
                .send(Cmd::Expire {
                    now,
                    budget: budget - removed as usize,
                    reply: tx,
                })
                .is_ok()
            {
                if let Ok(n) = rx.recv() {
                    removed += n;
                }
            }
        }
        removed
    }

    /// Total number of live keys across all shards.
    pub fn dbsize(&self) -> usize {
        self.stats().0
    }

    /// Total approximate memory across all shards.
    pub fn total_mem(&self) -> usize {
        self.stats().2
    }

    /// Number of live keys that carry an expire timestamp.
    pub fn expires_count(&self) -> usize {
        self.stats().1
    }

    fn query_config(&self) -> Option<(Option<usize>, MaxMemoryPolicy)> {
        let (tx, rx) = std::sync::mpsc::channel();
        if let Err(e) = self.shards[0].tx.send(Cmd::MaxMemory { reply: tx }) {
            tracing::error!(error = %e, "query_config send failed");
            return None;
        }
        match rx.recv() {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::error!(error = %e, "query_config actor disconnected");
                None
            }
        }
    }

    /// Aggregate (keys, expires, mem) across all shards via broadcast.
    pub(crate) fn stats(&self) -> (usize, usize, usize) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let keys = Arc::new(AtomicUsize::new(0));
        let expires = Arc::new(AtomicUsize::new(0));
        let mem = Arc::new(AtomicUsize::new(0));
        let (k2, e2, m2) = (keys.clone(), expires.clone(), mem.clone());
        self.broadcast(move |_id, sh| {
            k2.fetch_add(sh.dict.len(), Ordering::SeqCst);
            e2.fetch_add(
                sh.dict
                    .values()
                    .filter(|en| en.meta.expire_at_ms.is_some())
                    .count(),
                Ordering::SeqCst,
            );
            m2.fetch_add(sh.mem, Ordering::SeqCst);
        });
        (
            keys.load(Ordering::SeqCst),
            expires.load(Ordering::SeqCst),
            mem.load(Ordering::SeqCst),
        )
    }

    /// try_send + oneshot reply for the hot path. `pub(crate)` for the
    /// cross-shard coordinator's parallel MGET fan-out. `aof` controls sink
    /// appending on the owning actor (true for client writes, false for
    /// recovery replay / read-only coordinator probes).
    pub(crate) fn try_exec_on(&self, sid: usize, args: Vec<Bytes>, aof: bool) -> RespValue {
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        if let Err(e) = self.shards[sid].tx.try_send(Cmd::Exec {
            args,
            reply: reply_tx,
            aof,
        }) {
            return match e {
                TrySendError::Full(_) => {
                    crate::stats::inc(&crate::stats::STATS.queue_full);
                    RespValue::err("ERR shard command queue full")
                }
                TrySendError::Disconnected(_) => RespValue::err("ERR shard actor crashed"),
            };
        }
        match reply_rx.recv() {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => e,
            Err(_) => RespValue::err("ERR shard actor crashed"),
        }
    }

    /// try_send + oneshot reply for non-Exec shard commands (DelKey/Insert/
    /// FetchRemove). Maps channel errors like `try_exec_on`: Full →
    /// `ERR shard command queue full`, Disconnected → `ERR shard actor crashed`.
    fn try_send_cmd<T>(&self, sid: usize, f: impl FnOnce(Reply<T>) -> Cmd) -> Result<T, RespValue> {
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        if let Err(e) = self.shards[sid].tx.try_send(f(reply_tx)) {
            return Err(match e {
                TrySendError::Full(_) => {
                    crate::stats::inc(&crate::stats::STATS.queue_full);
                    RespValue::err("ERR shard command queue full")
                }
                TrySendError::Disconnected(_) => RespValue::err("ERR shard actor crashed"),
            });
        }
        match reply_rx.recv() {
            Ok(v) => Ok(v),
            Err(_) => Err(RespValue::err("ERR shard actor crashed")),
        }
    }

    fn config_cmd(&self, a: &[Bytes]) -> RespValue {
        if a.is_empty() {
            return RespValue::err("ERR wrong number of arguments for 'config' command");
        }
        let sub = String::from_utf8_lossy(&a[0]).to_ascii_uppercase();
        match sub.as_str() {
            "GET" => {
                if a.len() != 2 {
                    return RespValue::err(
                        "ERR wrong number of arguments for 'config|get' command",
                    );
                }
                let param = String::from_utf8_lossy(&a[1]).to_ascii_lowercase();
                match param.as_str() {
                    "maxmemory" => {
                        let v = self
                            .maxmemory()
                            .map(|m| m.to_string())
                            .unwrap_or_else(|| "0".into());
                        config_pair(&param, &v)
                    }
                    "maxmemory-policy" => config_pair(&param, self.maxmemory_policy().as_str()),
                    "appendonly" => config_pair(&param, "no"),
                    other => RespValue::err(format!("ERR unsupported CONFIG parameter '{other}'")),
                }
            }
            "SET" => {
                if a.len() != 3 {
                    return RespValue::err(
                        "ERR wrong number of arguments for 'config|set' command",
                    );
                }
                let param = String::from_utf8_lossy(&a[1]).to_ascii_lowercase();
                let val = String::from_utf8_lossy(&a[2]).to_string();
                match param.as_str() {
                    "maxmemory" => {
                        let n: usize = match val.parse() {
                            Ok(n) => n,
                            Err(_) => {
                                return RespValue::err(
                                    "ERR value is not an integer or out of range",
                                )
                            }
                        };
                        let policy = self.maxmemory_policy();
                        match self.set_maxmemory(n, policy) {
                            Ok(()) => RespValue::ok(),
                            Err(e) => e,
                        }
                    }
                    "maxmemory-policy" => {
                        let policy = match MaxMemoryPolicy::parse(&val) {
                            Some(p) => p,
                            None => {
                                return RespValue::err(format!(
                                    "ERR invalid maxmemory-policy '{val}'"
                                ))
                            }
                        };
                        match self.set_maxmemory_policy(policy) {
                            Ok(()) => RespValue::ok(),
                            Err(e) => e,
                        }
                    }
                    "appendonly" => RespValue::ok(),
                    other => RespValue::err(format!("ERR unsupported CONFIG parameter '{other}'")),
                }
            }
            "RESETSTAT" => RespValue::ok(),
            other => RespValue::err(format!("ERR unsupported CONFIG subcommand '{other}'")),
        }
    }

    fn rename_cmd(&self, a: &[Bytes], aof: bool) -> RespValue {
        if a.len() != 2 {
            return RespValue::err("ERR wrong number of arguments for 'rename' command");
        }
        let src = &a[0];
        let dst = &a[1];
        let sid_s = self.shard_id_for_key(src);
        let sid_d = self.shard_id_for_key(dst);
        if sid_s == sid_d {
            return self.try_exec_on(
                sid_s,
                vec![Bytes::from_static(b"RENAME"), src.clone(), dst.clone()],
                aof,
            );
        }
        // Cross-shard (non-atomic, matches current engine + spec §3.3):
        // fetch+remove src → del dst → insert src entry at dst.
        let now = self.now();
        let entry = match self.try_send_cmd(sid_s, |reply| Cmd::FetchRemove {
            key: src.clone(),
            now,
            reply,
        }) {
            Ok(e) => e,
            Err(e) => return e,
        };
        let Some((value, meta)) = entry else {
            return RespValue::err("ERR no such key");
        };
        // Build AOF payloads before Insert moves value/meta.
        let expire_at = meta.expire_at_ms;
        let dst_cmd = value_aof_cmd(dst, &value, &meta);
        if let Err(e) = self.try_send_cmd(sid_d, |reply| Cmd::DelKey {
            key: dst.clone(),
            now,
            reply,
        }) {
            return e;
        }
        if let Err(e) = self.try_send_cmd(sid_d, |reply| Cmd::Insert {
            key: dst.clone(),
            value,
            meta,
            reply,
        }) {
            return e;
        }
        if aof {
            // Per-shard records: DEL src on the source shard; on the
            // destination shard, DEL dst **before** the value command so
            // recovery cannot replay RPUSH/HSET/SADD/ZADD/SET on top of a
            // pre-existing dst container. Recovery replays each shard file
            // independently — no cross-shard ordering required.
            self.append_aof(sid_s, &[Bytes::from_static(b"DEL"), src.clone()]);
            self.append_aof(sid_d, &[Bytes::from_static(b"DEL"), dst.clone()]);
            self.append_aof(sid_d, &dst_cmd);
            if let Some(exp) = expire_at {
                self.append_aof(
                    sid_d,
                    &[
                        Bytes::from_static(b"PEXPIREAT"),
                        dst.clone(),
                        Bytes::from(exp.to_string()),
                    ],
                );
            }
        }
        RespValue::ok()
    }

    fn del_cmd(&self, a: &[Bytes], aof: bool) -> RespValue {
        if a.is_empty() {
            return RespValue::int(0);
        }
        let now = self.now();
        // Group keys by owning shard so we can log one DEL per shard after
        // every DelKey succeeded (buffer-then-write; partial failure returns
        // early without appending — MVP window documented in report).
        let mut per_shard: Vec<Vec<Bytes>> = vec![Vec::new(); self.shard_count];
        for k in a {
            per_shard[self.shard_id_for_key(k)].push(k.clone());
        }
        let mut n = 0i64;
        for (sid, keys) in per_shard.iter().enumerate() {
            if keys.is_empty() {
                continue;
            }
            for k in keys {
                match self.try_send_cmd(sid, |reply| Cmd::DelKey {
                    key: k.clone(),
                    now,
                    reply,
                }) {
                    Ok(i) => n += i,
                    Err(e) => return e,
                }
            }
        }
        if aof {
            for (sid, keys) in per_shard.iter().enumerate() {
                if keys.is_empty() {
                    continue;
                }
                let mut cmd = Vec::with_capacity(keys.len() + 1);
                cmd.push(Bytes::from_static(b"DEL"));
                cmd.extend(keys.iter().cloned());
                self.append_aof(sid, &cmd);
            }
        }
        RespValue::int(n)
    }

    fn exists_cmd(&self, a: &[Bytes]) -> RespValue {
        let mut n = 0i64;
        for k in a {
            let sid = self.shard_id_for_key(k);
            let resp = self.try_exec_on(sid, vec![Bytes::from_static(b"EXISTS"), k.clone()], false);
            match resp {
                RespValue::Integer(i) => n += i,
                RespValue::Error(m) => return RespValue::err(m),
                _ => {}
            }
        }
        RespValue::int(n)
    }

    fn mset_cmd(&self, a: &[Bytes], aof: bool) -> RespValue {
        if a.is_empty() || a.len() % 2 != 0 {
            return RespValue::err("ERR wrong number of arguments for 'mset' command");
        }
        let mut groups: Vec<Vec<(Bytes, Bytes)>> = vec![Vec::new(); self.shard_count];
        for pair in a.chunks(2) {
            let sid = self.shard_id_for_key(&pair[0]);
            groups[sid].push((pair[0].clone(), pair[1].clone()));
        }
        // Buffer-then-write: execute every shard with aof=false; append the
        // per-shard MSET subsets only after all shards succeeded so a later
        // shard failure cannot leave a partial MSET in the command log.
        let mut subs: Vec<(usize, Vec<Bytes>)> = Vec::new();
        for (sid, pairs) in groups.into_iter().enumerate() {
            if pairs.is_empty() {
                continue;
            }
            let mut sub = vec![Bytes::from_static(b"MSET")];
            for (k, v) in pairs {
                sub.push(k);
                sub.push(v);
            }
            if let RespValue::Error(m) = self.try_exec_on(sid, sub.clone(), false) {
                return RespValue::err(m);
            }
            subs.push((sid, sub));
        }
        if aof {
            for (sid, sub) in subs {
                self.append_aof(sid, &sub);
            }
        }
        RespValue::ok()
    }

    fn msetnx_cmd(&self, a: &[Bytes], aof: bool) -> RespValue {
        if a.is_empty() || a.len() % 2 != 0 {
            return RespValue::err("ERR wrong number of arguments for 'msetnx' command");
        }
        // Check all keys exist on their owning shards first.
        for pair in a.chunks(2) {
            let sid = self.shard_id_for_key(&pair[0]);
            let resp = self.try_exec_on(
                sid,
                vec![Bytes::from_static(b"EXISTS"), pair[0].clone()],
                false,
            );
            if let RespValue::Integer(1) = resp {
                return RespValue::int(0);
            }
            if let RespValue::Error(m) = resp {
                return RespValue::err(m);
            }
        }
        match self.mset_cmd(a, aof) {
            RespValue::SimpleString(_) => RespValue::int(1),
            other => other,
        }
    }
}

fn config_pair(param: &str, val: &str) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(Bytes::copy_from_slice(param.as_bytes())),
        RespValue::bulk(Bytes::copy_from_slice(val.as_bytes())),
    ])
}

/// Build the replayable value command for an entry (used when cross-shard
/// RENAME must log the destination write on the destination shard's AOF).
/// Mirrors the command shapes `rewrite_aof` emits.
fn value_aof_cmd(key: &Bytes, value: &JxValue, _meta: &KeyMeta) -> Vec<Bytes> {
    match value {
        JxValue::Str(b) => vec![Bytes::from_static(b"SET"), key.clone(), b.clone()],
        JxValue::Int(n) => vec![
            Bytes::from_static(b"SET"),
            key.clone(),
            Bytes::from(n.to_string()),
        ],
        JxValue::Hash(h) => {
            let mut cmd = vec![Bytes::from_static(b"HSET"), key.clone()];
            for (f, val) in h {
                cmd.push(f.clone());
                cmd.push(val.clone());
            }
            cmd
        }
        JxValue::List(l) => {
            let mut cmd = vec![Bytes::from_static(b"RPUSH"), key.clone()];
            for x in l {
                cmd.push(x.clone());
            }
            cmd
        }
        JxValue::Set(s) => {
            let mut cmd = vec![Bytes::from_static(b"SADD"), key.clone()];
            for x in s {
                cmd.push(x.clone());
            }
            cmd
        }
        JxValue::ZSet(z) => {
            let mut cmd = vec![Bytes::from_static(b"ZADD"), key.clone()];
            for (member, score) in &z.entries {
                cmd.push(Bytes::from(score.to_string()));
                cmd.push(member.clone());
            }
            cmd
        }
    }
}
