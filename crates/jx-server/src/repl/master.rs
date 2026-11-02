//! Master replication: backlog tee + repl-port listener.
//!
//! Wire snapshot envelope (after a `Fullresync` frame):
//! `[u32 BE snapshot_len][snapshot bytes]` followed by any backlog frames
//! retained from the FULLRESYNC offset, then the live frame stream.
//!
//! FULLRESYNC carries a real RDB snapshot: `jx_persist::snapshot_to_bytes`
//! serializes the master engine at handshake time. The backlog **offset is
//! captured before** the snapshot, then the backlog slice from that earlier
//! offset is sent after the snapshot. Writes teed in the snapshot window are
//! therefore never lost (they remain in the backlog from the earlier offset);
//! the residual is double-apply (same command in snapshot and backlog) for
//! non-idempotent writes — see runbook RPO.
//!
//! If `snapshot_to_bytes` fails, the replica connection is closed **without**
//! sending a Fullresync/empty snapshot; the replica retries fullsync. An empty
//! snapshot must never be treated as success (it would flush the replica).
//!
//! The repl listener binds to the configured `bind` address (not a hardcoded
//! `0.0.0.0`). A periodic `Heartbeat(current_offset)` task (~1s) keeps
//! replica-side offsets fresh when idle.
//!
//! Handshake accepted on connect:
//! - RESP `AUTH <pass>` (required when `requirepass_sm3` is configured) then
//!   RESP `REPLICAOF sync`
//! - or a raw text line starting with `REPLICAOF` (e.g. `REPLICAOF sync`)
//! - or a decoded `Fullresync` frame (type `0x03`)

use crate::repl::backlog::Backlog;
use crate::repl::codec::{decode_frames, encode_frame, ReplFrame};
use crate::repl::{is_write_cmd, repl_auth_ok};
use bytes::{Bytes, BytesMut};
use jx_engine::actor::ShardRuntime;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};

/// One connected replica: bounded queue of pre-encoded wire frames.
struct ReplicaConn {
    id: u64,
    tx: mpsc::Sender<Bytes>,
}

/// Master replication state: backlog, global write offset, live replicas.
pub struct MasterRepl {
    backlog: Mutex<Backlog>,
    offset: AtomicU64,
    replicas: Mutex<Vec<ReplicaConn>>,
    repl_id: String,
    #[allow(dead_code)]
    now_ms: fn() -> u64,
    bound_port: AtomicU16,
    next_id: AtomicU64,
    /// Signals the accept + heartbeat tasks to exit (demote teardown).
    stop_tx: broadcast::Sender<()>,
}

impl MasterRepl {
    pub fn new(backlog_size: usize, repl_id: impl Into<String>) -> Self {
        let (stop_tx, _) = broadcast::channel(1);
        Self {
            backlog: Mutex::new(Backlog::new(backlog_size)),
            offset: AtomicU64::new(0),
            replicas: Mutex::new(Vec::new()),
            repl_id: repl_id.into(),
            now_ms: jx_engine::system_now_ms,
            bound_port: AtomicU16::new(0),
            next_id: AtomicU64::new(1),
            stop_tx,
        }
    }

    /// Deterministic id for unit tests.
    pub fn new_for_test(backlog_size: usize) -> Self {
        Self::new(backlog_size, "testrepl00000001")
    }

    /// Stop the repl listener + heartbeat tasks (demote / role switch away
    /// from master). Connected replicas are dropped; no lingering FULLRESYNC
    /// source remains.
    pub fn shutdown(&self) {
        let _ = self.stop_tx.send(());
    }

    /// Tee one client command into the replication backlog and fan out to
    /// streaming replicas. No-op for non-write commands.
    pub fn on_write(&self, args: &[Bytes]) {
        if args.is_empty() || !is_write_cmd(args) {
            return;
        }
        let payload = jx_persist::encode_command(args);
        let frame = encode_frame(&ReplFrame::Command(Bytes::from(payload)));
        {
            let mut bl = self.backlog.lock().unwrap();
            let off = self.offset.load(Ordering::SeqCst);
            bl.append(off, &frame);
            self.offset
                .store(off + frame.len() as u64, Ordering::SeqCst);
        }
        let mut reps = self.replicas.lock().unwrap();
        // try_send failure (full/closed) marks the replica disconnected.
        reps.retain_mut(|r| r.tx.try_send(frame.clone()).is_ok());
    }

    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::SeqCst)
    }

    pub fn connected_replicas(&self) -> usize {
        self.replicas.lock().unwrap().len()
    }

    pub fn repl_id(&self) -> &str {
        &self.repl_id
    }

    /// Actual bound repl port (non-zero after a successful bind; 0 if bind failed).
    pub fn repl_port(&self) -> u16 {
        self.bound_port.load(Ordering::SeqCst)
    }

    /// Copy of backlog bytes from `offset`, if still retained.
    pub fn backlog_slice_from(&self, offset: u64) -> Option<Vec<u8>> {
        self.backlog
            .lock()
            .unwrap()
            .slice_from(offset)
            .map(|s| s.to_vec())
    }

    fn register_replica(&self, tx: mpsc::Sender<Bytes>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.replicas.lock().unwrap().push(ReplicaConn { id, tx });
        id
    }

    fn unregister_replica(&self, id: u64) {
        self.replicas.lock().unwrap().retain(|r| r.id != id);
    }
}

/// Spawn the master repl-port listener. `repl_port = 0` binds an ephemeral port.
///
/// The listener binds to `bind` (the node's configured client bind address),
/// not a hardcoded `0.0.0.0`.
///
/// `requirepass_sm3`, when set, is the repl-port AUTH secret (same value as
/// the client `requirepass-sm3`; replicas present the matching `masterauth`).
///
/// Returns the shared [`MasterRepl`] even if bind fails (writes still tee to
/// the backlog; no replica connections are accepted).
pub fn spawn_master_repl(
    rt: Arc<ShardRuntime>,
    bind: &str,
    repl_port: u16,
    backlog_size: usize,
    now_ms: fn() -> u64,
    requirepass_sm3: Option<String>,
) -> Arc<MasterRepl> {
    let (stop_tx, _) = broadcast::channel::<()>(1);
    let mut stop_rx = stop_tx.subscribe();
    let master = Arc::new(MasterRepl {
        backlog: Mutex::new(Backlog::new(backlog_size)),
        offset: AtomicU64::new(0),
        replicas: Mutex::new(Vec::new()),
        repl_id: gen_repl_id(),
        now_ms,
        bound_port: AtomicU16::new(0),
        next_id: AtomicU64::new(1),
        stop_tx,
    });
    let addr = format!("{bind}:{repl_port}");
    let std_listener = match std::net::TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("master repl bind {addr} failed: {e}");
            return master;
        }
    };
    if let Err(e) = std_listener.set_nonblocking(true) {
        tracing::error!("master repl set_nonblocking failed: {e}");
        return master;
    }
    let listener = match TcpListener::from_std(std_listener) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("master repl tokio listener from_std failed: {e}");
            return master;
        }
    };
    let port = listener.local_addr().map(|a| a.port()).unwrap_or(repl_port);
    master.bound_port.store(port, Ordering::SeqCst);
    let bound_disp = listener
        .local_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|_| bind.to_string());
    tracing::info!(
        "master repl listening on {bound_disp}:{port} repl_id={}",
        master.repl_id
    );

    // Periodic Heartbeat(current_offset) so replicas keep a fresh
    // master_repl_offset when idle (repl-timeout relies on this).
    {
        let m = master.clone();
        let mut hb_rx = master.stop_tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = hb_rx.recv() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        let off = m.offset();
                        let frame = encode_frame(&ReplFrame::Heartbeat(off));
                        let mut reps = m.replicas.lock().unwrap();
                        reps.retain_mut(|r| r.tx.try_send(frame.clone()).is_ok());
                    }
                }
            }
        });
    }

    let m = master.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop_rx.recv() => {
                    tracing::info!("master repl listener shutting down");
                    break;
                }
                acc = listener.accept() => match acc {
                    Ok((stream, peer)) => {
                        let m = m.clone();
                        let auth = requirepass_sm3.clone();
                        let rt = rt.clone();
                        tokio::spawn(handle_replica(stream, peer.to_string(), m, auth, rt));
                    }
                    Err(e) => {
                        tracing::error!("master repl accept error: {e}");
                        break;
                    }
                },
            }
        }
    });
    master
}

/// Per-replica task: handshake → FULLRESYNC + RDB snapshot → stream frames.
async fn handle_replica(
    mut stream: TcpStream,
    peer: String,
    master: Arc<MasterRepl>,
    requirepass_sm3: Option<String>,
    rt: Arc<ShardRuntime>,
) {
    match read_handshake(&mut stream, requirepass_sm3.as_deref()).await {
        Ok(true) => {}
        Ok(false) => {
            tracing::debug!("replica {peer} handshake failed");
            return;
        }
        Err(e) => {
            tracing::debug!("replica {peer} handshake failed: {e}");
            return;
        }
    }
    // Capture the backlog offset BEFORE the snapshot so writes teed during
    // snapshot capture stay in the backlog slice (no loss window). Residual:
    // double-apply of non-idempotent writes present in both snapshot and
    // backlog — documented in the runbook RPO section.
    let offset = master.offset();
    // Real FULLRESYNC: engine snapshot (RDB bytes) at handshake time.
    // On snapshot error: close the connection (never send Fullresync with an
    // empty snapshot — that would flush the replica and treat failure as
    // success). Replica retries fullsync on disconnect.
    let snapshot = match jx_persist::snapshot_to_bytes(&rt, None) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("replica {peer} snapshot failed: {e}; closing without FULLRESYNC");
            return;
        }
    };
    let backlog = master.backlog_slice_from(offset);
    let full = encode_frame(&ReplFrame::Fullresync {
        repl_id: master.repl_id.clone(),
        offset,
    });
    if stream.write_all(&full).await.is_err() {
        tracing::debug!("replica {peer} fullresync write failed");
        return;
    }
    // Snapshot envelope: u32 BE length + raw RDB bytes.
    if stream
        .write_all(&(snapshot.len() as u32).to_be_bytes())
        .await
        .is_err()
    {
        tracing::debug!("replica {peer} snapshot-len write failed");
        return;
    }
    if !snapshot.is_empty() && stream.write_all(&snapshot).await.is_err() {
        tracing::debug!("replica {peer} snapshot write failed");
        return;
    }
    if let Some(bl) = backlog {
        if !bl.is_empty() && stream.write_all(&bl).await.is_err() {
            tracing::debug!("replica {peer} backlog write failed");
            return;
        }
    }
    let (tx, mut rx) = mpsc::channel::<Bytes>(256);
    let id = master.register_replica(tx);
    tracing::info!(
        "replica {peer} connected (id={id}, repl_id={}, offset={offset}, snap={})",
        master.repl_id,
        snapshot.len()
    );
    while let Some(frame) = rx.recv().await {
        if stream.write_all(&frame).await.is_err() {
            break;
        }
    }
    master.unregister_replica(id);
    tracing::info!("replica {peer} disconnected (id={id})");
}

/// Read until a valid handshake is detected or the 5s budget expires.
///
/// Accepts RESP `AUTH` + `REPLICAOF`, raw text `REPLICAOF ...`, or a
/// `Fullresync` frame. Returns `Ok(true)` on success.
async fn read_handshake(stream: &mut TcpStream, requirepass: Option<&str>) -> anyhow::Result<bool> {
    let mut buf = BytesMut::new();
    let mut authed = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut tmp = [0u8; 1024];
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("handshake timeout");
        }
        // RESP commands (AUTH / REPLICAOF) at the head of the buffer.
        while let Some(args) = try_take_resp_array(&mut buf)? {
            let name = args
                .first()
                .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase())
                .unwrap_or_default();
            match name.as_str() {
                "AUTH" => {
                    let presented = args
                        .get(1)
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .unwrap_or_default();
                    let ok = match requirepass {
                        Some(secret) => repl_auth_ok(secret, &presented),
                        None => true,
                    };
                    authed = ok;
                    let resp = if ok {
                        "+OK\r\n"
                    } else {
                        "-WRONGPASS invalid password\r\n"
                    };
                    stream.write_all(resp.as_bytes()).await?;
                    stream.flush().await?;
                    if !ok {
                        anyhow::bail!("repl AUTH failed");
                    }
                }
                "REPLICAOF" => {
                    if requirepass.is_some() && !authed {
                        anyhow::bail!("NOAUTH Authentication required.");
                    }
                    return Ok(true);
                }
                "PING" => {
                    stream.write_all(b"+PONG\r\n").await?;
                    stream.flush().await?;
                }
                other => anyhow::bail!("unexpected handshake command {other}"),
            }
        }
        // Legacy raw-text handshake: line starting with REPLICAOF.
        if try_take_text_replicaof(&mut buf) {
            if requirepass.is_some() && !authed {
                anyhow::bail!("NOAUTH Authentication required.");
            }
            return Ok(true);
        }
        // Decoded Fullresync frame (client-initiated resync request).
        {
            let mut probe = buf.clone();
            if let Ok(frames) = decode_frames(&mut probe) {
                if frames
                    .iter()
                    .any(|f| matches!(f, ReplFrame::Fullresync { .. }))
                {
                    let _ = decode_frames(&mut buf);
                    if requirepass.is_some() && !authed {
                        anyhow::bail!("NOAUTH Authentication required.");
                    }
                    return Ok(true);
                }
            }
        }
        if buf.len() > 64 * 1024 {
            anyhow::bail!("handshake too large");
        }
        let n = match tokio::time::timeout(remaining, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => anyhow::bail!("eof during handshake"),
            Ok(Ok(n)) => n,
            _ => anyhow::bail!("read failed during handshake"),
        };
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// If `buf` starts with a complete RESP array, consume it and return args.
fn try_take_resp_array(buf: &mut BytesMut) -> anyhow::Result<Option<Vec<Bytes>>> {
    if buf.is_empty() || buf[0] != b'*' {
        return Ok(None);
    }
    // Skip the '*' prefix; parse_line_int reads the digits only.
    let mut i = 1usize;
    let count = match parse_line_int(buf, &mut i)? {
        Some(n) => n,
        None => return Ok(None),
    };
    if !(0..=64).contains(&count) {
        anyhow::bail!("bad array length {count}");
    }
    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if i >= buf.len() || buf[i] != b'$' {
            anyhow::bail!("expected bulk string in handshake array");
        }
        i += 1;
        let len = match parse_line_int(buf, &mut i)? {
            Some(n) => n,
            None => return Ok(None),
        };
        if !(0..=1024 * 1024).contains(&len) {
            anyhow::bail!("bad bulk length {len}");
        }
        let len = len as usize;
        if i + len + 2 > buf.len() {
            return Ok(None);
        }
        args.push(Bytes::copy_from_slice(&buf[i..i + len]));
        i += len + 2;
    }
    let _ = buf.split_to(i);
    Ok(Some(args))
}

/// Consume a raw text line starting with REPLICAOF (legacy handshake).
fn try_take_text_replicaof(buf: &mut BytesMut) -> bool {
    let Ok(text) = std::str::from_utf8(buf) else {
        return false;
    };
    let upper = text.trim_start().to_ascii_uppercase();
    if !upper.starts_with("REPLICAOF") {
        return false;
    }
    match text.find('\n') {
        Some(pos) => {
            let _ = buf.split_to(pos + 1);
            true
        }
        None => false,
    }
}

/// Parse `*<n>\r\n` or `$<n>\r\n` at `*i`, leaving `*i` just past the newline.
fn parse_line_int(buf: &[u8], i: &mut usize) -> anyhow::Result<Option<i64>> {
    let start = *i;
    while *i < buf.len() && buf[*i] != b'\n' {
        *i += 1;
    }
    if *i >= buf.len() {
        *i = start;
        return Ok(None);
    }
    if *i == start || buf[*i - 1] != b'\r' {
        anyhow::bail!("bad line ending in handshake");
    }
    let s = std::str::from_utf8(&buf[start..*i - 1]).map_err(|_| anyhow::anyhow!("bad utf8"))?;
    let n: i64 = s
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("bad integer"))?;
    *i += 1;
    Ok(Some(n))
}

fn gen_repl_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{nanos:016x}")
}
