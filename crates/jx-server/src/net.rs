use crate::background;
use crate::config::{Config, ConfigRuntime, Role, TlcpDistidPolicy, TlsMode};
use crate::conn;
use crate::persistence_cmd::PersistenceCtx;
use crate::repl::master::spawn_master_repl;
use crate::repl::replica::spawn_replica;
use crate::repl::ReplHandle;
use anyhow::{Context, Result};
use jx_engine::actor::{spawn_shard_runtime, ShardRuntime};
use jx_engine::MaxMemoryPolicy;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

#[derive(Clone)]
enum ServerAcceptor {
    Plain,
    Tls(tokio_rustls::TlsAcceptor),
    Tlcp(Arc<jx_crypto::TlcpAcceptor>),
}

/// Live handles returned once the accept loop stops (shutdown signal or error).
pub struct ServerHandle {
    pub runtime: Arc<ShardRuntime>,
    pub key: Option<[u8; 32]>,
    pub bg: tokio::task::JoinHandle<()>,
    pub pctx: PersistenceCtx,
    /// Master repl state when role is Master (None otherwise).
    pub master: Option<Arc<crate::repl::MasterRepl>>,
    /// Shared repl handles (master tee + live replica task) for shutdown.
    pub repl: ReplHandle,
}

/// Wait for SIGINT (ctrl_c) or SIGTERM (unix). Used by `run_server` when no
/// external shutdown channel is supplied.
async fn wait_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    "failed to install SIGTERM handler ({e}); falling back to ctrl_c only"
                );
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            r = tokio::signal::ctrl_c() => {
                if let Err(e) = r {
                    tracing::warn!("ctrl_c wait failed: {e}");
                }
            }
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Spawn a task that signals shutdown when SIGINT/SIGTERM arrives.
fn spawn_shutdown_signal() -> broadcast::Receiver<()> {
    let (tx, rx) = broadcast::channel::<()>(1);
    tokio::spawn(async move {
        wait_shutdown_signal().await;
        let _ = tx.send(());
    });
    rx
}

async fn wait_shutdown_rx(rx: &mut broadcast::Receiver<()>) {
    loop {
        match rx.recv().await {
            Ok(_) => break,
            Err(broadcast::error::RecvError::Closed) => break,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
        }
    }
}

fn build_tls_acceptor(cfg: &Config) -> Result<tokio_rustls::TlsAcceptor> {
    let paths = cfg
        .tls
        .as_ref()
        .context("tls-mode tls requires tls-cert/tls-key")?;
    let certs = {
        let mut r = std::io::BufReader::new(std::fs::File::open(&paths.cert)?);
        rustls_pemfile::certs(&mut r)
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("parse tls cert pem")?
    };
    if certs.is_empty() {
        anyhow::bail!("tls cert {} contains no certificates", paths.cert.display());
    }
    let key = {
        let mut r = std::io::BufReader::new(std::fs::File::open(&paths.key)?);
        rustls_pemfile::private_key(&mut r)
            .context("parse tls key pem")?
            .context("tls key pem contains no private key")?
    };
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .context("tls protocol versions")?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("tls single cert")?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

fn build_tlcp_acceptor(cfg: &Config) -> Result<jx_crypto::TlcpAcceptor> {
    let t = cfg
        .tlcp
        .as_ref()
        .context("tls-mode tlcp requires tlcp-* certs")?;
    let tlcp_cfg = jx_crypto::TlcpConfig::from_pem_files(
        &t.sign_cert,
        &t.sign_key,
        &t.enc_cert,
        &t.enc_key,
        &t.ca,
    )?;
    let policy = match t.distid_policy {
        TlcpDistidPolicy::Strict => jx_crypto::TlcpDistidPolicy::Strict,
        TlcpDistidPolicy::PermissiveOpenSsl => jx_crypto::TlcpDistidPolicy::PermissiveOpenSsl,
    };
    jx_crypto::TlcpAcceptor::from_config_with_policy(&tlcp_cfg, policy)
}

/// Run the server until a shutdown signal (SIGINT/SIGTERM) or fatal accept
/// error, then return live handles for persistence shutdown. Accept errors
/// are logged and returned as `Err` (not treated as graceful shutdown).
pub async fn run_server(cfg: Config) -> Result<ServerHandle> {
    let rx = spawn_shutdown_signal();
    run_server_with_shutdown(cfg, rx).await
}

/// Run the server until `shutdown` fires (or the channel is closed). Testable
/// variant: callers drive shutdown via the broadcast sender.
pub async fn run_server_with_shutdown(
    cfg: Config,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<ServerHandle> {
    std::fs::create_dir_all(&cfg.dir)?;
    let nshards = cfg.shards_or_default(
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
    );
    // Bounded per-shard actor channels: capacity = maxclients (min 1).
    let chan_cap = if cfg.maxclients == 0 {
        1024
    } else {
        cfg.maxclients as usize
    };
    let rt = Arc::new(spawn_shard_runtime(
        nshards,
        chan_cap,
        jx_engine::system_now_ms,
    ));
    // Apply policy at startup even when maxmemory is unset; maxmemory 0 means unlimited.
    let policy = match cfg.maxmemory_policy.as_str() {
        "allkeys-lru" => MaxMemoryPolicy::AllKeysLru,
        "volatile-lru" => MaxMemoryPolicy::VolatileLru,
        "allkeys-lfu" => MaxMemoryPolicy::AllKeysLfu,
        "volatile-lfu" => MaxMemoryPolicy::VolatileLfu,
        "volatile-ttl" => MaxMemoryPolicy::VolatileTtl,
        _ => MaxMemoryPolicy::NoEviction,
    };
    rt.set_maxmemory_policy(policy)
        .map_err(|e| anyhow::anyhow!("apply maxmemory policy at startup: {e:?}"))?;
    if let Some(mm) = cfg.maxmemory {
        rt.set_maxmemory(mm, policy)
            .map_err(|e| anyhow::anyhow!("apply maxmemory at startup: {e:?}"))?;
    }
    let runtime = Arc::new(RwLock::new(ConfigRuntime::from_startup(&cfg)));
    let key = match &cfg.storage_key_file {
        Some(p) => Some(jx_crypto::load_keyfile(p)?),
        None => None,
    };
    // Recovery policy (C1 fix, matches original design "AOF 优先于 RDB"):
    // when appendonly is on and combined `appendonly.aof` exists, that AOF
    // (a full-state rewrite stream from shutdown/BGREWRITEAOF) is the base —
    // RDB is skipped so full-state commands are not double-applied on top of
    // the same snapshot. Otherwise RDB is the base; per-shard append files
    // replay as deltas when appendonly is on. Replay failure refuses startup.
    //
    // Shard-count mismatch: RDB files are shard-partitioned; `load_rdb`
    // returns Err when the file's shard count differs from this runtime's
    // (message includes both counts). Changing `shards` between runs requires
    // a fresh SAVE or a shard-agnostic AOF-only recovery.
    jx_persist::recover_on_startup(&rt, &cfg.dir, key.as_ref(), cfg.appendonly)?;
    // Attach the actor-owned AOF sink only after recovery (replay must not
    // re-append). Plaintext only: encrypted durability is RDB + rewrite
    // full-stream seal (FileAofSink MVP limit — live encrypted append cannot
    // accumulate a command stream). fsync policy comes from `appendfsync`.
    if cfg.appendonly && key.is_none() {
        let fsync = jx_persist::FsyncPolicy::parse(&cfg.appendfsync);
        rt.set_aof_sink(Some(Arc::new(jx_persist::FileAofSink::with_fsync(
            cfg.dir.clone(),
            fsync,
        ))));
    }
    let bg = background::spawn_background(rt.clone());
    let pctx = PersistenceCtx::new(rt.clone(), cfg.dir.clone(), key);

    // Master role: repl-port listener + write tee shared with client conns.
    // Standalone/replica: None (no tee). requirepass-sm3 also gates repl-port
    // AUTH (replicas present matching `masterauth`).
    //
    // Replica role: connect to master repl channel after the client port is up.
    // `replicaof host port` is the master client port; master repl-port
    // defaults to client port + 1 (same convention as Config::repl_port).
    // Both handles live in a shared `ReplHandle` cloned into every conn for
    // INFO fill and REPLICAOF role switching.
    let repl = ReplHandle {
        master: std::sync::Arc::new(std::sync::Mutex::new(if cfg.role == Role::Master {
            Some(spawn_master_repl(
                rt.clone(),
                &cfg.bind,
                cfg.repl_port,
                cfg.backlog_size,
                jx_engine::system_now_ms,
                cfg.requirepass_sm3.clone(),
            ))
        } else {
            None
        })),
        replica: std::sync::Arc::new(std::sync::Mutex::new(if cfg.role == Role::Replica {
            cfg.replicaof.as_ref().map(|(host, port)| {
                spawn_replica(
                    rt.clone(),
                    host.clone(),
                    port.saturating_add(1),
                    cfg.masterauth.clone(),
                    cfg.repl_timeout_secs,
                )
            })
        } else {
            None
        })),
    };

    let acceptor = match cfg.tls_mode {
        TlsMode::Plain => ServerAcceptor::Plain,
        TlsMode::Tls => {
            // Fail-closed: bad/missing cert or key refuses startup; no plain fallback.
            let a = build_tls_acceptor(&cfg)
                .context("tls acceptor setup failed (tls-mode tls; refusing to start)")?;
            ServerAcceptor::Tls(a)
        }
        TlsMode::Tlcp => {
            let a = build_tlcp_acceptor(&cfg).context("tlcp acceptor setup failed")?;
            ServerAcceptor::Tlcp(Arc::new(a))
        }
    };

    let addr = format!("{}:{}", cfg.bind, cfg.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(
        "listening on {addr} tls-mode={:?} shards={nshards}",
        cfg.tls_mode
    );
    loop {
        let (stream, peer) = tokio::select! {
            acc = listener.accept() => match acc {
                Ok(x) => x,
                Err(e) => {
                    tracing::error!("accept error: {e}");
                    // Fatal: surface to the caller instead of masquerading as
                    // graceful shutdown. main.rs propagates the error.
                    return Err(anyhow::anyhow!("accept error: {e}"));
                }
            },
            _ = wait_shutdown_rx(&mut shutdown) => {
                tracing::info!("shutdown signal received; stopping accept loop");
                repl.stop_replica();
                return Ok(ServerHandle {
                    runtime: rt,
                    key: pctx.key,
                    bg,
                    pctx,
                    master: repl.master(),
                    repl,
                });
            }
        };
        let rt = rt.clone();
        let runtime = runtime.clone();
        let acceptor = acceptor.clone();
        let pctx = pctx.clone();
        let repl = repl.clone();
        jx_engine::stats::inc(&jx_engine::stats::STATS.conn_received);
        tokio::spawn(async move {
            let result = match acceptor {
                ServerAcceptor::Plain => {
                    conn::handle_conn(stream, rt, runtime, Some(pctx), repl, 1024).await
                }
                ServerAcceptor::Tls(a) => match a.accept(stream).await {
                    Ok(tls) => conn::handle_conn(tls, rt, runtime, Some(pctx), repl, 1024).await,
                    Err(e) => {
                        tracing::debug!("tls handshake failed for {peer}: {e}");
                        jx_engine::stats::inc(&jx_engine::stats::STATS.rejected_connections);
                        Ok(())
                    }
                },
                ServerAcceptor::Tlcp(a) => match a.accept(stream).await {
                    Ok(s) => conn::handle_conn(s, rt, runtime, Some(pctx), repl, 1024).await,
                    Err(e) => {
                        tracing::debug!("tlcp handshake failed for {peer}: {e}");
                        jx_engine::stats::inc(&jx_engine::stats::STATS.rejected_connections);
                        Ok(())
                    }
                },
            };
            if let Err(e) = result {
                tracing::debug!("conn {peer} closed: {e}");
            }
        });
    }
}
