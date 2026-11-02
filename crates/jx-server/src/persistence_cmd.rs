use bytes::Bytes;
use jx_engine::actor::ShardRuntime;
use jx_proto::RespValue;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub fn unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Context for SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF, intercepted at the conn
/// layer so `jx-engine` stays free of a `jx-persist` dependency.
#[derive(Clone)]
pub struct PersistenceCtx {
    pub runtime: Arc<ShardRuntime>,
    pub dir: PathBuf,
    pub key: Option<[u8; 32]>,
    /// Unix seconds of the last successful RDB write; 0 until first save.
    pub last_save: Arc<AtomicI64>,
    /// Single-flight guard shared by SAVE/BGSAVE/shutdown. BGSAVE `try_lock`s
    /// (busy → Redis-style error); SAVE/shutdown block until they can claim.
    pub save_lock: Arc<Mutex<()>>,
    /// True while a SAVE/BGSAVE/shutdown RDB write is claimed or running.
    save_in_progress: Arc<AtomicBool>,
}

impl PersistenceCtx {
    pub fn new(runtime: Arc<ShardRuntime>, dir: PathBuf, key: Option<[u8; 32]>) -> Self {
        Self {
            runtime,
            dir,
            key,
            last_save: Arc::new(AtomicI64::new(0)),
            save_lock: Arc::new(Mutex::new(())),
            save_in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    fn mark_saved(&self) {
        self.last_save.store(unix_secs(), Ordering::SeqCst);
    }

    /// Claim the single-flight save slot. Returns `None` when a save is
    /// already in flight (caller should wait and retry).
    fn try_claim(&self) -> Option<()> {
        let _g = self.save_lock.lock().unwrap_or_else(|p| p.into_inner());
        if self.save_in_progress.swap(true, Ordering::SeqCst) {
            None
        } else {
            Some(())
        }
    }

    fn release_claim(&self) {
        self.save_in_progress.store(false, Ordering::SeqCst);
    }

    /// Best-effort graceful-shutdown persist: rewrite AOF when appendonly is
    /// on, then always write RDB. Updates last_save on success. Waits for any
    /// in-flight SAVE/BGSAVE via the shared single-flight slot.
    ///
    /// AOF retention on shutdown:
    /// - `appendonly yes`: `rewrite_aof` consolidates live state into the
    ///   combined AOF (and clears shard files); RDB is written next. Combined
    ///   AOF is kept so the rewritten command stream remains available; shard
    ///   files cleared by rewrite hold any post-rewrite ops and replay after
    ///   combined on recovery.
    /// - `appendonly no`: RDB is the recovery source; leftover AOF files are
    ///   compacted away so a later `appendonly yes` start cannot double-apply.
    pub fn shutdown(&self, appendonly: bool) -> anyhow::Result<()> {
        while self.try_claim().is_none() {
            std::thread::sleep(Duration::from_millis(2));
        }
        let result: anyhow::Result<()> = (|| {
            let rdb = self.dir.join("dump.rdb");
            if appendonly {
                let aof = self.dir.join("appendonly.aof");
                jx_persist::rewrite_aof(&self.runtime, &aof, self.key.as_ref())
                    .map_err(|e| anyhow::anyhow!("rewrite aof on shutdown: {e:#}"))?;
                // Keep the just-rewritten combined AOF; do not compact.
                jx_persist::save_rdb(&self.runtime, &rdb, self.key.as_ref())
                    .map_err(|e| anyhow::anyhow!("save rdb on shutdown: {e:#}"))?;
            } else {
                jx_persist::save_rdb_compact_aof(&self.runtime, &rdb, self.key.as_ref(), &self.dir)
                    .map_err(|e| anyhow::anyhow!("save rdb on shutdown: {e:#}"))?;
            }
            Ok(())
        })();
        self.release_claim();
        result?;
        self.mark_saved();
        Ok(())
    }
}

/// Handle SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF. Returns `None` when `name` is
/// not one of these commands so the caller can fall through to dispatch.
pub fn try_handle(name: &str, ctx: &PersistenceCtx, args: &[Bytes]) -> Option<RespValue> {
    let _ = args;
    match name {
        "SAVE" => Some(save_sync(ctx)),
        "BGSAVE" => Some(bgsave(ctx)),
        "LASTSAVE" => Some(RespValue::int(ctx.last_save.load(Ordering::SeqCst))),
        "BGREWRITEAOF" => Some(bgrewriteaof(ctx)),
        _ => None,
    }
}

fn save_sync(ctx: &PersistenceCtx) -> RespValue {
    while ctx.try_claim().is_none() {
        std::thread::sleep(Duration::from_millis(2));
    }
    let path = ctx.dir.join("dump.rdb");
    // After RDB success, compact shard/combined AOF so recovery does not
    // double-apply (see jx_persist::compact_aof_after_save).
    let result = jx_persist::save_rdb_compact_aof(&ctx.runtime, &path, ctx.key.as_ref(), &ctx.dir);
    ctx.release_claim();
    if let Err(e) = result {
        return RespValue::err(format!("ERR save failed: {e:#}"));
    }
    ctx.mark_saved();
    RespValue::ok()
}

fn bgsave(ctx: &PersistenceCtx) -> RespValue {
    // try_lock + claim under the guard: if another saver holds the lock or
    // already claimed the slot, reject with the Redis-style error.
    match ctx.save_lock.try_lock() {
        Ok(guard) => {
            if ctx.save_in_progress.swap(true, Ordering::SeqCst) {
                return RespValue::err("ERR Background save already in progress");
            }
            drop(guard);
            let ctx = ctx.clone();
            std::thread::spawn(move || {
                let _g = ctx.save_lock.lock().unwrap_or_else(|p| p.into_inner());
                let path = ctx.dir.join("dump.rdb");
                match jx_persist::save_rdb_compact_aof(
                    &ctx.runtime,
                    &path,
                    ctx.key.as_ref(),
                    &ctx.dir,
                ) {
                    Ok(()) => {
                        ctx.mark_saved();
                        tracing::debug!("bgsave complete: {}", path.display());
                    }
                    Err(e) => tracing::warn!("bgsave failed: {e:#}"),
                }
                ctx.release_claim();
            });
            RespValue::ok()
        }
        Err(_) => RespValue::err("ERR Background save already in progress"),
    }
}

fn bgrewriteaof(ctx: &PersistenceCtx) -> RespValue {
    // Works even when appendonly is off: rewrite writes the AOF file anyway.
    let ctx = ctx.clone();
    std::thread::spawn(move || {
        let path = ctx.dir.join("appendonly.aof");
        match jx_persist::rewrite_aof(&ctx.runtime, &path, ctx.key.as_ref()) {
            Ok(()) => tracing::debug!("bgrewriteaof complete: {}", path.display()),
            Err(e) => tracing::warn!("bgrewriteaof failed: {e:#}"),
        }
    });
    RespValue::ok()
}
