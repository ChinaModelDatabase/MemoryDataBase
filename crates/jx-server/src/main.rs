use anyhow::Context;
use jx_server::config::load_config;
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/MemoryDataBase.conf".to_string());
    let cfg = load_config(Path::new(&path))?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&cfg.loglevel))
        .json()
        .init();
    // run_server blocks until SIGINT/SIGTERM, then returns live handles.
    let handle = jx_server::run_server(cfg.clone()).await?;
    tracing::info!("graceful shutdown: persisting");
    jx_server::shutdown_persist_blocking(&handle.pctx, &cfg)
        .await
        .context("shutdown persist")?;
    handle.bg.abort();
    tracing::info!("shutdown complete");
    Ok(())
}
