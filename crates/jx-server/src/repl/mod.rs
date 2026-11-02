use bytes::Bytes;
use std::sync::{Arc, Mutex};

use crate::config::Config;

pub mod backlog;
pub mod codec;
pub mod master;
pub mod replica;

pub use master::MasterRepl;
pub use replica::{spawn_replica, ReplicaHandle, ReplicaState};

/// Replication role of this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Standalone,
    Master,
    Replica,
}

/// Shared replication handles cloned into each connection for `INFO`
/// replication fill and `REPLICAOF` role switching.
#[derive(Clone, Default)]
pub struct ReplHandle {
    /// Master write-tee / repl-port state. `None` when not a master; lazily
    /// spawned on `REPLICAOF NO ONE` promotion (shared across conn clones).
    pub master: Arc<Mutex<Option<Arc<MasterRepl>>>>,
    /// Background replica sync task, if one is running.
    pub replica: Arc<Mutex<Option<ReplicaHandle>>>,
}

impl ReplHandle {
    /// Stop and clear any running replica sync task.
    pub fn stop_replica(&self) {
        let old = self.replica.lock().unwrap().take();
        if let Some(h) = old {
            h.stop();
        }
    }

    /// Install `h` as the replica task, stopping any previous one.
    pub fn set_replica(&self, h: ReplicaHandle) {
        let old = self.replica.lock().unwrap().replace(h);
        if let Some(old) = old {
            old.stop();
        }
    }

    /// Install `m` as the master tee (lazy-spawn path on promotion).
    pub fn set_master(&self, m: Arc<MasterRepl>) {
        *self.master.lock().unwrap() = Some(m);
    }

    /// Demote teardown: stop the master repl listener + heartbeat tasks and
    /// clear `repl.master` so writes are no longer teed and no lingering
    /// FULLRESYNC source remains.
    pub fn clear_master(&self) {
        let old = self.master.lock().unwrap().take();
        if let Some(m) = old {
            m.shutdown();
        }
    }

    /// Current master tee, if any.
    pub fn master(&self) -> Option<Arc<MasterRepl>> {
        self.master.lock().unwrap().clone()
    }
}

/// `# Replication` INFO section text (trailing `\r\n` included).
///
/// Offsets/slaves come from the master backlog or replica stream state.
/// Master nodes also report `master_repl_port:` (bound repl port, or the
/// configured `repl-port` when the listener is not yet bound).
pub fn info_replication_section(
    role: Role,
    master: Option<&MasterRepl>,
    replica: Option<ReplicaState>,
    repl_port: u16,
) -> String {
    let role_str = match role {
        Role::Master => "master",
        Role::Replica => "replica",
        Role::Standalone => "standalone",
    };
    let (offset, slaves) = match role {
        Role::Master => master
            .map(|m| (m.offset(), m.connected_replicas()))
            .unwrap_or((0, 0)),
        Role::Replica => replica.map(|s| (s.master_offset, 0)).unwrap_or((0, 0)),
        Role::Standalone => (0, 0),
    };
    let mut out = format!(
        "# Replication\r\nrole:{role_str}\r\nmaster_repl_offset:{offset}\r\nconnected_slaves:{slaves}\r\n"
    );
    if role == Role::Master {
        let bound = master.map(|m| m.repl_port()).unwrap_or(0);
        out.push_str(&format!(
            "master_repl_port:{}\r\n",
            if bound != 0 { bound } else { repl_port }
        ));
    }
    out
}

/// Replication settings extracted from [`Config`] for future repl modules.
#[derive(Debug, Clone)]
pub struct ReplConfig {
    pub role: Role,
    pub replicaof: Option<(String, u16)>,
    pub repl_port: u16,
    pub backlog_size: usize,
    pub repl_timeout_secs: u64,
    pub masterauth: Option<String>,
}

impl Config {
    pub fn repl_config(&self) -> ReplConfig {
        ReplConfig {
            role: self.role,
            replicaof: self.replicaof.clone(),
            repl_port: self.repl_port,
            backlog_size: self.backlog_size,
            repl_timeout_secs: self.repl_timeout_secs,
            masterauth: self.masterauth.clone(),
        }
    }
}

/// Write-command predicate shared by the master tee and replica READONLY gate.
///
/// Mirrors the engine AOF list plus DEL/FLUSHALL/FLUSHDB/RENAME, which mutate
/// state via non-`Exec` paths and still must reach replicas / be rejected on a
/// read-only replica. `jx_engine`'s `is_write_cmd` is private, so this lives
/// here.
pub fn is_write_cmd(args: &[Bytes]) -> bool {
    let Some(name) = args.first() else {
        return false;
    };
    let n = std::str::from_utf8(name).unwrap_or("").to_ascii_uppercase();
    matches!(
        n.as_str(),
        "SET"
            | "SETNX"
            | "SETEX"
            | "PSETEX"
            | "MSET"
            | "MSETNX"
            | "APPEND"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "RENAME"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "PERSIST"
            | "DEL"
            | "FLUSHALL"
            | "FLUSHDB"
            | "HSET"
            | "HDEL"
            | "HINCRBY"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "LSET"
            | "SADD"
            | "SREM"
            | "SPOP"
            | "ZADD"
            | "ZREM"
            | "ZINCRBY"
    )
}

/// Verify a repl-port AUTH presented password against the configured secret.
///
/// Accepts a direct string match (secret stored as-is) or the existing
/// requirepass-sm3 verification (unsalted hex or `salthex:hashhex`).
pub fn repl_auth_ok(secret: &str, presented: &str) -> bool {
    if secret == presented {
        return true;
    }
    crate::conn::verify_requirepass(secret, presented)
}
