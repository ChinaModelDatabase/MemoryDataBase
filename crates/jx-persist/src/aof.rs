use anyhow::{Context, Result};
use bytes::Bytes;
use jx_engine::actor::{AofSink, ShardRuntime};
use jx_engine::JxValue;
use jx_proto::{encode_to_bytes, RespDecoder, RespValue};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// AOF 追加接口（MVP）。
///
/// 明文模式（`key` 为 None）：每次调用向文件追加一条 RESP 命令数组，支持多命令流。
/// 加密模式（`key` 为 Some）：文件格式为 `JXAOFENC` + sm4_gcm_seal(单条命令)。
/// **MVP 限制**：实时加密 `append_aof` 不支持多命令追加——每次 append 会覆盖整个文件为单条命令。
/// `rewrite_aof` 不受此限制：先在内存构建完整明文命令流，再一次性 seal 为单个 JXAOFENC 载荷。
/// 加密持久化亦可使用 `save_rdb`。
pub fn encode_command(args: &[Bytes]) -> Vec<u8> {
    encode_to_bytes(&RespValue::Array(Some(
        args.iter().map(|a| RespValue::bulk(a.clone())).collect(),
    )))
    .to_vec()
}

/// Per-shard AOF append file path: `{dir}/appendonly.{sid}.aof`.
pub fn shard_aof_path(dir: &Path, sid: usize) -> PathBuf {
    dir.join(format!("appendonly.{sid}.aof"))
}

/// `appendfsync` policy for live per-shard AOF appends.
///
/// - [`FsyncPolicy::Always`]: `sync_all` after every successful append.
/// - [`FsyncPolicy::EverySec`]: flush on append; a background thread attached
///   to each sink `sync_all`s existing shard files once per second (default).
/// - [`FsyncPolicy::No`]: flush only (page-cache); fastest, largest loss
///   window on OS crash.
///
/// Config values (`always|everysec|no`) are validated in jx-server
/// `config.rs`; [`FsyncPolicy::parse`] maps them for the sink.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

impl FsyncPolicy {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "always" => Self::Always,
            "no" => Self::No,
            // "everysec" and unknown values fall back to the default policy.
            _ => Self::EverySec,
        }
    }
}

/// `sync_all` every existing per-shard append file `{dir}/appendonly.{sid}.aof`.
fn sync_shard_aof_files(dir: &Path) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    let t0 = std::time::Instant::now();
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(mid) = name
            .strip_prefix("appendonly.")
            .and_then(|r| r.strip_suffix(".aof"))
        else {
            continue;
        };
        if !mid.is_empty() && mid.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(f) = std::fs::File::open(entry.path()) {
                f.sync_all().ok();
            }
        }
    }
    jx_engine::stats::record_aof_flush(t0.elapsed().as_micros() as u64);
}

/// Actor-owned AOF sink: each shard actor appends successful write commands
/// to its own `appendonly.{sid}.aof` (plaintext RESP, one file per shard).
///
/// **fsync:** see [`FsyncPolicy`]. `EverySec` spawns a once-per-second sync
/// thread that is stopped on `Drop`.
///
/// **Encrypted mode:** live per-shard append is intentionally not wired — the
/// MVP `append_aof` seal path overwrites the file with a single command
/// (cannot accumulate a stream). With `storage-key-file` set the server does
/// not attach this sink; durability comes from encrypted `save_rdb` and
/// `rewrite_aof` (full-stream seal once). See ACCEPTANCE known gaps.
pub struct FileAofSink {
    dir: PathBuf,
    policy: FsyncPolicy,
    stop: Option<Arc<AtomicBool>>,
    flusher: Option<std::thread::JoinHandle<()>>,
}

impl FileAofSink {
    /// Default sink: [`FsyncPolicy::EverySec`] (background flusher thread).
    pub fn new(dir: PathBuf) -> Self {
        Self::with_fsync(dir, FsyncPolicy::EverySec)
    }

    /// Sink with an explicit [`FsyncPolicy`]. `EverySec` spawns a background
    /// thread that syncs existing shard files once per second until drop.
    pub fn with_fsync(dir: PathBuf, policy: FsyncPolicy) -> Self {
        let (stop, flusher) = if policy == FsyncPolicy::EverySec {
            let stop = Arc::new(AtomicBool::new(false));
            let stop2 = stop.clone();
            let dir2 = dir.clone();
            let h = std::thread::spawn(move || {
                // Poll stop every 100ms so Drop can join without waiting out
                // a full 1s sleep; still sync on the everysec cadence.
                let mut slept_ms = 0u32;
                while !stop2.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(100));
                    slept_ms += 100;
                    if slept_ms >= 1000 {
                        sync_shard_aof_files(&dir2);
                        slept_ms = 0;
                    }
                }
                sync_shard_aof_files(&dir2);
            });
            (Some(stop), Some(h))
        } else {
            (None, None)
        };
        Self {
            dir,
            policy,
            stop,
            flusher,
        }
    }

    pub fn policy(&self) -> FsyncPolicy {
        self.policy
    }
}

impl Drop for FileAofSink {
    fn drop(&mut self) {
        if let Some(s) = &self.stop {
            s.store(true, Ordering::SeqCst);
        }
        if let Some(h) = self.flusher.take() {
            let _ = h.join();
        }
    }
}

impl AofSink for FileAofSink {
    fn append(&self, sid: usize, args: &[Bytes]) {
        if args.is_empty() {
            return;
        }
        let path = shard_aof_path(&self.dir, sid);
        if let Err(e) = append_aof(&path, args, None) {
            tracing::warn!(sid, error = %e, "per-shard aof append failed");
            return;
        }
        if self.policy == FsyncPolicy::Always {
            let t0 = std::time::Instant::now();
            if let Ok(f) = std::fs::File::open(&path) {
                f.sync_all().ok();
            }
            jx_engine::stats::record_aof_flush(t0.elapsed().as_micros() as u64);
        }
        // EverySec: flush already done in append_aof; background thread
        // handles sync_all. No: flush only.
    }
}

pub fn append_aof(path: &Path, args: &[Bytes], key: Option<&[u8; 32]>) -> Result<()> {
    let payload = encode_command(args);
    let body = match key {
        Some(k) => jx_crypto::sm4_gcm_seal(k, &payload)?,
        None => payload,
    };
    // MVP：整段加密时文件仅含一条；明文模式支持追加多条。
    if key.is_some() {
        let mut out = Vec::with_capacity(8 + body.len());
        out.extend_from_slice(b"JXAOFENC");
        out.extend_from_slice(&body);
        std::fs::write(path, out)?;
    } else {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        f.write_all(&body)?;
        f.flush()?;
    }
    Ok(())
}

pub fn replay_aof(rt: &ShardRuntime, path: &Path, key: Option<&[u8; 32]>) -> Result<u64> {
    replay_aof_inner(rt, path, key, None)
}

/// Replay per-shard AOF file for `sid` with shard-local FLUSHALL/FLUSHDB.
fn replay_aof_shard(
    rt: &ShardRuntime,
    path: &Path,
    key: Option<&[u8; 32]>,
    sid: usize,
) -> Result<u64> {
    replay_aof_inner(rt, path, key, Some(sid))
}

fn replay_aof_inner(
    rt: &ShardRuntime,
    path: &Path,
    key: Option<&[u8; 32]>,
    replay_sid: Option<usize>,
) -> Result<u64> {
    let raw = std::fs::read(path).context("read aof")?;
    let data = if raw.starts_with(b"JXAOFENC") {
        jx_crypto::sm4_gcm_open(key.context("aof encrypted but no key")?, &raw[8..])?
    } else {
        raw
    };
    let mut decoder = RespDecoder::new();
    decoder.feed(&data);
    let mut n = 0u64;
    while let Some(v) = decoder.next_value().context("aof resp decode")? {
        if let RespValue::Array(Some(items)) = v {
            let args: Vec<Bytes> = items
                .into_iter()
                .filter_map(|x| match x {
                    RespValue::BulkString(Some(b)) => Some(b),
                    _ => None,
                })
                .collect();
            if !args.is_empty() {
                // Per-shard replay: FLUSHALL/FLUSHDB is shard-local (clear only
                // the shard whose file is being replayed). Live path still
                // broadcasts; replay must not, because every shard file logs
                // its own FLUSHALL and a higher-sid file's replay would wipe
                // post-flush writes from lower-sid files.
                let name = std::str::from_utf8(&args[0])
                    .unwrap_or("")
                    .to_ascii_uppercase();
                match (replay_sid, name.as_str()) {
                    (Some(sid), "FLUSHALL" | "FLUSHDB") => {
                        rt.flush_shard_local(sid);
                    }
                    _ => {
                        // exec_no_aof: recovery must not re-append replayed
                        // commands to per-shard AOF files.
                        let _ = rt.exec_no_aof(args);
                    }
                }
                n += 1;
            }
        }
    }
    Ok(n)
}

/// Replay every existing per-shard AOF append file `{dir}/appendonly.{sid}.aof`
/// for `sid in 0..rt.shard_count()`. Returns total commands replayed.
///
/// Recovery order (server `recover_on_startup`): combined AOF as base when
/// present + appendonly, else RDB; then per-shard append files on top;
/// (appendonly) consolidate via `rewrite_aof` so shard files are cleared and
/// the next crash cannot double-replay non-idempotent ops.
///
/// **FLUSHALL during this replay is shard-local** (see `replay_aof_inner`).
pub fn replay_aof_shards(rt: &ShardRuntime, dir: &Path, key: Option<&[u8; 32]>) -> Result<u64> {
    let mut n = 0u64;
    for sid in 0..rt.shard_count() {
        let path = shard_aof_path(dir, sid);
        if path.exists() {
            n += replay_aof_shard(rt, &path, key, sid)?;
        }
    }
    Ok(n)
}

/// Startup recovery base policy (C1 fix).
///
/// When `appendonly` is true and combined `appendonly.aof` exists, the
/// combined AOF is treated as the **recovery base** (Redis-like; original
/// design "AOF 优先于 RDB"): skip RDB load, replay the AOF stream, then
/// replay per-shard delta files on top.
///
/// Rationale: `rewrite_aof` (shutdown / BGREWRITEAOF / recovery consolidation)
/// writes a **full-state** command stream. Shutdown with `appendonly yes`
/// writes both that stream and an RDB snapshot of the same moment. Loading
/// RDB then replaying the full-state AOF on top double-applies non-idempotent
/// ops (RPUSH doubles a list; INCR increments twice).
///
/// When combined AOF does not exist (e.g. after SAVE compact via
/// `save_rdb_compact_aof`), RDB is the base and per-shard files are deltas
/// on top — only replayed when `appendonly` is true.
///
/// After per-shard deltas are replayed, a consolidating `rewrite_aof` clears
/// shard files so the next crash cannot double-replay. AOF replay failure
/// returns `Err` (refuse startup).
pub fn recover_on_startup(
    rt: &ShardRuntime,
    dir: &Path,
    key: Option<&[u8; 32]>,
    appendonly: bool,
) -> Result<()> {
    let aof = dir.join("appendonly.aof");
    let rdb = dir.join("dump.rdb");
    if appendonly && aof.exists() {
        // Combined AOF is the base (full-state rewrite stream). Skip RDB.
        replay_aof(rt, &aof, key)?;
        // Per-shard incremental appends on top of the AOF base.
        if replay_aof_shards(rt, dir, key)? > 0 {
            rewrite_aof(rt, &aof, key)?;
        }
    } else {
        if rdb.exists() {
            crate::rdb::load_rdb(rt, &rdb, key)?;
        }
        if appendonly && replay_aof_shards(rt, dir, key)? > 0 {
            rewrite_aof(rt, &aof, key)?;
        }
    }
    Ok(())
}

/// Remove per-shard AOF append files (`appendonly.{sid}.aof`, numeric sid
/// only — never the combined `appendonly.aof`). Called after `rewrite_aof`
/// (state is now consolidated into the combined AOF), by `compact_aof_after_save`
/// (RDB is now the recovery source of truth), and by server recovery after a
/// consolidating rewrite.
///
/// **Known race:** a write appended between snapshot collection and this
/// clear is lost from AOF (RDB save rules / next rewrite still capture live
/// state). MVP accepts this window; `appendfsync` everysec bounds exposure.
pub fn clear_shard_aof_files(dir: &Path) -> Result<()> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Ok(());
    };
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(mid) = name
            .strip_prefix("appendonly.")
            .and_then(|r| r.strip_suffix(".aof"))
        else {
            continue;
        };
        if !mid.is_empty() && mid.chars().all(|c| c.is_ascii_digit()) {
            std::fs::remove_file(entry.path())
                .with_context(|| format!("remove {}", entry.path().display()))?;
        }
    }
    Ok(())
}

/// After a successful SAVE/BGSAVE RDB write: drop per-shard append files and
/// the combined `appendonly.aof`. The RDB is now the recovery source of truth;
/// replaying older AOF records on top would double-apply non-idempotent ops
/// (INCR) or resurrect keys (older SET/DEL). Used by server SAVE/BGSAVE/
/// shutdown via [`save_rdb_compact_aof`].
pub fn compact_aof_after_save(dir: &Path) -> Result<()> {
    clear_shard_aof_files(dir)?;
    let combined = dir.join("appendonly.aof");
    if combined.exists() {
        std::fs::remove_file(&combined).context("remove combined aof after save")?;
    }
    Ok(())
}

/// SAVE/BGSAVE helper: write RDB, then compact AOF files so crash-sim recovery
/// (load RDB only) does not double-apply ops already captured in the snapshot.
pub fn save_rdb_compact_aof(
    rt: &ShardRuntime,
    rdb_path: &Path,
    key: Option<&[u8; 32]>,
    aof_dir: &Path,
) -> Result<()> {
    crate::rdb::save_rdb(rt, rdb_path, key)?;
    compact_aof_after_save(aof_dir)
}

/// Snapshot every key as replayable commands, then write the AOF file.
///
/// - Emits the value command (SET/HSET/RPUSH/SADD/ZADD) followed by
///   `PEXPIREAT key ms` when the key has an expire timestamp, so TTLs survive
///   rewrite + replay.
/// - Plaintext (`key == None`): writes the command stream directly.
/// - Encrypted (`key == Some`): builds the full plaintext stream in memory,
///   then seals the entire stream once as a single `JXAOFENC` payload. The
///   previous behavior (per-command seal + overwrite) left only one command in
///   the rewritten file — that data-loss path is intentionally removed.
pub fn rewrite_aof(rt: &ShardRuntime, path: &Path, key: Option<&[u8; 32]>) -> Result<()> {
    let mut stream: Vec<u8> = Vec::new();
    for sid in 0..rt.shard_count() {
        for (k, v, m) in rt.snapshot_shard(sid) {
            let cmd: Vec<Bytes> = match v {
                JxValue::Str(b) => vec![Bytes::from_static(b"SET"), k.clone(), b],
                JxValue::Int(n) => {
                    vec![
                        Bytes::from_static(b"SET"),
                        k.clone(),
                        Bytes::from(n.to_string()),
                    ]
                }
                JxValue::Hash(h) => {
                    let mut cmd = vec![Bytes::from_static(b"HSET"), k.clone()];
                    for (f, val) in h {
                        cmd.push(f);
                        cmd.push(val);
                    }
                    cmd
                }
                JxValue::List(l) => {
                    let mut cmd = vec![Bytes::from_static(b"RPUSH"), k.clone()];
                    for x in l {
                        cmd.push(x);
                    }
                    cmd
                }
                JxValue::Set(s) => {
                    let mut cmd = vec![Bytes::from_static(b"SADD"), k.clone()];
                    for x in s {
                        cmd.push(x);
                    }
                    cmd
                }
                JxValue::ZSet(z) => {
                    let mut cmd = vec![Bytes::from_static(b"ZADD"), k.clone()];
                    for (member, score) in z.entries {
                        cmd.push(Bytes::from(format!("{score}")));
                        cmd.push(member);
                    }
                    cmd
                }
            };
            stream.extend_from_slice(&encode_command(&cmd));
            if let Some(exp) = m.expire_at_ms {
                let pexpire = vec![
                    Bytes::from_static(b"PEXPIREAT"),
                    k,
                    Bytes::from(exp.to_string()),
                ];
                stream.extend_from_slice(&encode_command(&pexpire));
            }
        }
    }
    let payload = match key {
        Some(k) => {
            let sealed = jx_crypto::sm4_gcm_seal(k, &stream).context("sm4 seal aof rewrite")?;
            let mut out = Vec::with_capacity(8 + sealed.len());
            out.extend_from_slice(b"JXAOFENC");
            out.extend_from_slice(&sealed);
            out
        }
        None => stream,
    };
    let tmp = path.with_extension("aof.tmp");
    if tmp.exists() {
        std::fs::remove_file(&tmp)?;
    }
    {
        let mut f = std::fs::File::create(&tmp).context("create aof tmp")?;
        f.write_all(&payload).context("write aof tmp")?;
        f.sync_all().ok();
    }
    std::fs::rename(&tmp, path).context("rename aof")?;
    // Consolidated: per-shard append files are now represented in the combined
    // AOF. Clear them so recovery cannot double-replay (see clear_shard_aof_files).
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            clear_shard_aof_files(dir).context("clear shard aof files after rewrite")?;
        }
    }
    Ok(())
}
