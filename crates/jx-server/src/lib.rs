pub mod background;
pub mod config;
pub mod conn;
pub mod dispatch;
pub mod net;
pub mod persistence_cmd;
pub mod repl;

pub use config::ConfigRuntime;
pub use net::{run_server, run_server_with_shutdown, ServerHandle};
pub use persistence_cmd::PersistenceCtx;

use anyhow::Context;

/// Graceful-shutdown persist via the shared `PersistenceCtx`: rewrite AOF when
/// `appendonly` is on, then always write RDB to `cfg.dir/dump.rdb`. Updates
/// the same `last_save` AtomicI64 used by SAVE/BGSAVE/LASTSAVE.
/// Aborting background tasks is the caller's job (see `ServerHandle::bg`).
pub fn shutdown_persist(pctx: &PersistenceCtx, cfg: &config::Config) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.dir).context("create dir for shutdown persist")?;
    pctx.shutdown(cfg.appendonly).context("shutdown persist")
}

/// Async variant of [`shutdown_persist`]: runs the blocking persist on
/// `tokio::task::spawn_blocking` so the async runtime is not stalled.
pub async fn shutdown_persist_blocking(
    pctx: &PersistenceCtx,
    cfg: &config::Config,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.dir).context("create dir for shutdown persist")?;
    let pctx = pctx.clone();
    let appendonly = cfg.appendonly;
    tokio::task::spawn_blocking(move || pctx.shutdown(appendonly))
        .await
        .map_err(|e| anyhow::anyhow!("shutdown persist task failed: {e}"))?
        .context("shutdown persist")
}
pub mod slowlog;
