use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

pub use crate::repl::Role;

/// Runtime-mutable config shared with connection tasks via `Arc<RwLock<_>>`.
/// Updated live by CONFIG SET; read by AUTH and future runtime consumers.
#[derive(Debug, Clone)]
pub struct ConfigRuntime {
    pub loglevel: String,
    pub timeout_secs: u64,
    /// Salted or unsalted requirepass-sm3 value (see conn.rs AUTH).
    pub requirepass_sm3: Option<String>,
    /// Actual configured appendonly flag (reported by CONFIG GET appendonly).
    pub appendonly: bool,
    /// Startup replication role; conn layer uses it for READONLY gating.
    pub role: Role,
    /// Live replicaof target `host port` (master client port). Cleared by
    /// `REPLICAOF NO ONE`; set by startup config or `REPLICAOF host port`.
    pub replicaof: Option<(String, u16)>,
    /// Repl-port AUTH secret carried into dynamic `REPLICAOF` spawns.
    pub masterauth: Option<String>,
    /// Configured master repl port (client port + 1 when unset). Used by the
    /// lazy-spawn master tee on `REPLICAOF NO ONE` promotion.
    pub repl_port: u16,
    /// Backlog size for the lazy-spawn master tee (from startup config).
    pub repl_backlog_size: usize,
    /// Client bind address; the master repl listener binds to the same host.
    pub bind: String,
    /// Idle replication-stream timeout (seconds) passed into dynamic
    /// `REPLICAOF` replica spawns.
    pub repl_timeout_secs: u64,
    /// Transport mode in effect for this listener (INFO security).
    pub tls_mode: TlsMode,
    /// Slowlog threshold in microseconds (0 disables).
    pub slowlog_log_slower_than: i64,
    /// Slowlog ring buffer capacity.
    pub slowlog_max_len: usize,
    /// Per-server slowlog ring buffer.
    pub slowlog: std::sync::Arc<crate::slowlog::Slowlog>,
}

impl ConfigRuntime {
    pub fn from_startup(cfg: &Config) -> Self {
        Self {
            loglevel: cfg.loglevel.clone(),
            timeout_secs: cfg.timeout_secs,
            requirepass_sm3: cfg.requirepass_sm3.clone(),
            appendonly: cfg.appendonly,
            role: cfg.role,
            replicaof: cfg.replicaof.clone(),
            masterauth: cfg.masterauth.clone(),
            repl_port: cfg.repl_port,
            repl_backlog_size: cfg.backlog_size,
            bind: cfg.bind.clone(),
            repl_timeout_secs: cfg.repl_timeout_secs,
            tls_mode: cfg.tls_mode,
            slowlog_log_slower_than: cfg.slowlog_log_slower_than,
            slowlog_max_len: cfg.slowlog_max_len,
            slowlog: std::sync::Arc::new(crate::slowlog::Slowlog::new(
                crate::slowlog::SlowlogConfig {
                    log_slower_than: cfg.slowlog_log_slower_than,
                    max_len: cfg.slowlog_max_len,
                },
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    Plain,
    Tls,
    Tlcp,
}

#[derive(Debug, Clone, Default)]
pub struct TlcpPaths {
    pub sign_cert: PathBuf,
    pub sign_key: PathBuf,
    pub enc_cert: PathBuf,
    pub enc_key: PathBuf,
    pub ca: PathBuf,
    /// SM2 signature distid policy for client chain verification.
    pub distid_policy: TlcpDistidPolicy,
}

/// How strictly SM2 certificate signatures are verified on TLCP handshakes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlcpDistidPolicy {
    /// GM/T standard distid only (fail-closed).
    #[default]
    Strict,
    /// Also accept the empty distid used by OpenSSL 3.x-issued certs.
    PermissiveOpenSsl,
}

#[derive(Debug, Clone, Default)]
pub struct TlsPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: String,
    pub port: u16,
    pub tls_mode: TlsMode,
    pub shards: Option<usize>,
    pub maxmemory: Option<usize>,
    pub maxmemory_policy: String,
    pub dir: PathBuf,
    pub save: Vec<(u64, u64)>,
    pub appendonly: bool,
    pub appendfsync: String,
    pub requirepass_sm3: Option<String>,
    pub storage_key_file: Option<PathBuf>,
    pub tlcp: Option<TlcpPaths>,
    pub tls: Option<TlsPaths>,
    pub maxclients: u32,
    pub timeout_secs: u64,
    pub loglevel: String,
    pub role: Role,
    pub replicaof: Option<(String, u16)>,
    pub repl_port: u16,
    pub backlog_size: usize,
    pub repl_timeout_secs: u64,
    pub masterauth: Option<String>,
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: usize,
}

fn parse_size(s: &str) -> Result<usize> {
    let s = s.trim().to_ascii_lowercase();
    let (num, mult) = if let Some(r) = s.strip_suffix("gb") {
        (r, 1024 * 1024 * 1024)
    } else if let Some(r) = s.strip_suffix("mb") {
        (r, 1024 * 1024)
    } else if let Some(r) = s.strip_suffix("kb") {
        (r, 1024)
    } else {
        (s.as_str(), 1)
    };
    let n: usize = num.trim().parse().context("bad size")?;
    Ok(n * mult)
}

pub fn load_config(path: &Path) -> Result<Config> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut cfg = Config {
        bind: "127.0.0.1".into(),
        port: 6379,
        tls_mode: TlsMode::Plain,
        shards: None,
        maxmemory: None,
        maxmemory_policy: "noeviction".into(),
        dir: PathBuf::from("./data"),
        save: vec![(900, 1), (300, 10), (60, 10000)],
        appendonly: false,
        appendfsync: "everysec".into(),
        requirepass_sm3: None,
        storage_key_file: None,
        tlcp: None,
        tls: None,
        maxclients: 10000,
        timeout_secs: 0,
        loglevel: "info".into(),
        role: Role::Standalone,
        replicaof: None,
        repl_port: 0,
        backlog_size: parse_size("64mb")?,
        repl_timeout_secs: 60,
        masterauth: None,
        slowlog_log_slower_than: 10000,
        slowlog_max_len: 128,
    };
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once(char::is_whitespace) else {
            bail!("config line {}: expected 'key value'", i + 1);
        };
        let (k, v) = (k.trim(), v.trim());
        match k {
            "bind" => cfg.bind = v.to_string(),
            "port" => cfg.port = v.parse().context("port")?,
            "tls-mode" => {
                cfg.tls_mode = match v {
                    "plain" => TlsMode::Plain,
                    "tls" => TlsMode::Tls,
                    "tlcp" => TlsMode::Tlcp,
                    other => bail!("bad tls-mode {other}"),
                };
            }
            "shards" => cfg.shards = Some(v.parse()?),
            "maxmemory" => cfg.maxmemory = Some(parse_size(v)?),
            "maxmemory-policy" => cfg.maxmemory_policy = v.to_string(),
            "dir" => cfg.dir = PathBuf::from(v),
            "appendonly" => cfg.appendonly = matches!(v, "yes" | "true" | "1"),
            "appendfsync" => cfg.appendfsync = v.to_string(),
            "requirepass-sm3" => cfg.requirepass_sm3 = Some(v.to_string()),
            "storage-key-file" => cfg.storage_key_file = Some(PathBuf::from(v)),
            "tlcp-sign-cert" => {
                cfg.tlcp.get_or_insert_with(Default::default).sign_cert = PathBuf::from(v)
            }
            "tlcp-sign-key" => {
                cfg.tlcp.get_or_insert_with(Default::default).sign_key = PathBuf::from(v)
            }
            "tlcp-enc-cert" => {
                cfg.tlcp.get_or_insert_with(Default::default).enc_cert = PathBuf::from(v)
            }
            "tlcp-enc-key" => {
                cfg.tlcp.get_or_insert_with(Default::default).enc_key = PathBuf::from(v)
            }
            "tlcp-ca" => cfg.tlcp.get_or_insert_with(Default::default).ca = PathBuf::from(v),
            "tlcp-distid-policy" => {
                cfg.tlcp.get_or_insert_with(Default::default).distid_policy = match v {
                    "strict" => TlcpDistidPolicy::Strict,
                    "permissive" => TlcpDistidPolicy::PermissiveOpenSsl,
                    other => bail!("bad tlcp-distid-policy {other} (strict|permissive)"),
                }
            }
            "tls-cert" => cfg.tls.get_or_insert_with(Default::default).cert = PathBuf::from(v),
            "tls-key" => cfg.tls.get_or_insert_with(Default::default).key = PathBuf::from(v),
            "maxclients" => cfg.maxclients = v.parse()?,
            "timeout" => cfg.timeout_secs = v.parse()?,
            "loglevel" => cfg.loglevel = v.to_string(),
            "role" => {
                cfg.role = match v {
                    "standalone" => Role::Standalone,
                    "master" => Role::Master,
                    "replica" => Role::Replica,
                    other => bail!("bad role {other}"),
                };
            }
            "replicaof" => {
                let mut parts = v.split_whitespace();
                let (Some(host), Some(port)) = (parts.next(), parts.next()) else {
                    bail!("replicaof requires 'host port'");
                };
                if parts.next().is_some() {
                    bail!("replicaof requires exactly 'host port'");
                }
                cfg.replicaof = Some((host.to_string(), port.parse().context("replicaof port")?));
            }
            "repl-port" => cfg.repl_port = v.parse().context("repl-port")?,
            "backlog-size" => cfg.backlog_size = parse_size(v)?,
            "repl-timeout" => cfg.repl_timeout_secs = v.parse().context("repl-timeout")?,
            "masterauth" => cfg.masterauth = Some(v.to_string()),
            "slowlog-log-slower-than" => {
                cfg.slowlog_log_slower_than = v.parse().context("slowlog-log-slower-than")?
            }
            "slowlog-max-len" => cfg.slowlog_max_len = v.parse().context("slowlog-max-len")?,
            "save" => {
                let mut parts = v.split_whitespace();
                if let (Some(a), Some(b)) = (parts.next(), parts.next()) {
                    cfg.save.push((a.parse()?, b.parse()?));
                }
            }
            other => bail!("unknown config key {other}"),
        }
    }
    if cfg.repl_port == 0 {
        cfg.repl_port = cfg.port.saturating_add(1);
    }
    cfg.validate()?;
    Ok(cfg)
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.port == 0 {
            bail!("port must be non-zero");
        }
        if !matches!(self.appendfsync.as_str(), "always" | "everysec" | "no") {
            bail!("appendfsync must be always|everysec|no");
        }
        match self.tls_mode {
            TlsMode::Tlcp => {
                let t = self
                    .tlcp
                    .as_ref()
                    .context("tls-mode tlcp requires tlcp-* certs")?;
                for p in [&t.sign_cert, &t.sign_key, &t.enc_cert, &t.enc_key, &t.ca] {
                    if !p.exists() {
                        bail!("missing tlcp file {}", p.display());
                    }
                }
            }
            TlsMode::Tls => {
                let t = self
                    .tls
                    .as_ref()
                    .context("tls-mode tls requires tls-cert/tls-key")?;
                if !t.cert.exists() || !t.key.exists() {
                    bail!("missing tls cert/key");
                }
            }
            TlsMode::Plain => {}
        }
        if let Some(k) = &self.storage_key_file {
            if !k.exists() {
                bail!("storage-key-file {} not found", k.display());
            }
        }
        Ok(())
    }

    pub fn shards_or_default(&self, cpu: usize) -> usize {
        self.shards.unwrap_or(cpu).max(1)
    }
}
