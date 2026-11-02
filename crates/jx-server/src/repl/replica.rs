//! Replica-side replication: connect to the master repl-port, fullsync,
//! then stream incremental frames.
//!
//! Handshake (after optional `AUTH` when `masterauth` is set):
//! RESP `REPLICAOF sync`. Master replies with a `Fullresync` frame, a
//! `u32` BE snapshot length + RDB snapshot bytes (real data via
//! `jx_persist::snapshot_to_bytes`), then streams `Command` / `Heartbeat`
//! frames (including any backlog frames retained from the fullsync offset).
//!
//! Fullsync: empty snapshot flushes local state via broadcast FLUSHALL;
//! non-empty snapshot loads through `jx_persist::load_rdb_bytes`. Incremental
//! `Command` frames apply via `ShardRuntime::exec_no_aof` (bypasses the
//! conn-layer READONLY gate). Applied frames advance `master_offset` by the
//! teed frame size; `Heartbeat` frames carry the master's current offset.
//!
//! Idle stream reads time out after `repl_timeout_secs` (0 disables) and the
//! connection is dropped so `sync_loop` reconnects / re-FULLRESYNCs. A
//! mid-stream `Fullresync` is treated as a reconnect trigger (not a
//! flush-and-stream without snapshot).

use crate::repl::codec::{decode_frames, ReplFrame};
use bytes::{Buf, Bytes, BytesMut};
use jx_engine::actor::ShardRuntime;
use jx_proto::{RespDecoder, RespValue};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

/// Snapshot of replica replication state (INFO-facing in later tasks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaState {
    pub master_offset: u64,
    pub status: String,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self {
            master_offset: 0,
            status: "offline".into(),
        }
    }
}

/// Handle to a background replica sync task.
pub struct ReplicaHandle {
    shared: Arc<RwLock<ReplicaState>>,
    stop: broadcast::Sender<()>,
    join: tokio::task::JoinHandle<()>,
}

impl ReplicaHandle {
    /// Point-in-time snapshot of replica state.
    pub fn state(&self) -> ReplicaState {
        self.shared.read().unwrap().clone()
    }

    /// Signal the sync task to stop and abort it.
    pub fn stop(&self) {
        let _ = self.stop.send(());
        self.join.abort();
    }
}

/// Spawn the replica sync task.
///
/// `master_port` is the master's **repl port** (not the client port). Callers
/// that only know the master's client port (config `replicaof host port`)
/// should pass `port + 1`, matching the default master `repl-port` convention.
///
/// `repl_timeout_secs` is the idle read timeout on the replication stream
/// (0 disables); on expiry the task disconnects and reconnects.
pub fn spawn_replica(
    rt: Arc<ShardRuntime>,
    master_host: String,
    master_port: u16,
    masterauth: Option<String>,
    repl_timeout_secs: u64,
) -> ReplicaHandle {
    let shared = Arc::new(RwLock::new(ReplicaState::default()));
    let (stop, stop_rx) = broadcast::channel::<()>(1);
    let shared2 = shared.clone();
    let join = tokio::spawn(async move {
        sync_loop(
            rt,
            master_host,
            master_port,
            masterauth,
            repl_timeout_secs,
            shared2,
            stop_rx,
        )
        .await;
    });
    ReplicaHandle { shared, stop, join }
}

fn set_status(shared: &Arc<RwLock<ReplicaState>>, status: &str, offset: Option<u64>) {
    let mut g = shared.write().unwrap();
    g.status = status.to_string();
    if let Some(o) = offset {
        g.master_offset = o;
    }
}

async fn sync_loop(
    rt: Arc<ShardRuntime>,
    master_host: String,
    master_port: u16,
    masterauth: Option<String>,
    repl_timeout_secs: u64,
    shared: Arc<RwLock<ReplicaState>>,
    mut stop_rx: broadcast::Receiver<()>,
) {
    let addr = format!("{master_host}:{master_port}");
    let mut backoff_ms = 50u64;
    loop {
        set_status(&shared, "connecting", None);
        match run_once(
            &rt,
            &addr,
            masterauth.as_deref(),
            repl_timeout_secs,
            &shared,
            &mut stop_rx,
        )
        .await
        {
            Ok(()) => {
                if stop_rx.try_recv().is_ok() {
                    return;
                }
                set_status(&shared, "connecting", None);
            }
            Err(e) => {
                tracing::warn!("replica sync to {addr} failed: {e}; retry in {backoff_ms}ms");
                set_status(&shared, "connecting", None);
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
            _ = stop_rx.recv() => return,
        }
        backoff_ms = (backoff_ms * 2).min(2_000);
    }
}

async fn read_stream_byte(
    stream: &mut TcpStream,
    tmp: &mut [u8],
    repl_timeout_secs: u64,
    stop_rx: &mut broadcast::Receiver<()>,
) -> anyhow::Result<usize> {
    if repl_timeout_secs == 0 {
        let n = tokio::select! {
            r = stream.read(tmp) => r?,
            _ = stop_rx.recv() => anyhow::bail!("stopped"),
        };
        return Ok(n);
    }
    let n = tokio::select! {
        r = tokio::time::timeout(Duration::from_secs(repl_timeout_secs), stream.read(tmp)) => {
            match r {
                Ok(Ok(n)) => n,
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => anyhow::bail!("replication idle timeout after {repl_timeout_secs}s"),
            }
        }
        _ = stop_rx.recv() => anyhow::bail!("stopped"),
    };
    Ok(n)
}

async fn run_once(
    rt: &Arc<ShardRuntime>,
    addr: &str,
    masterauth: Option<&str>,
    repl_timeout_secs: u64,
    shared: &Arc<RwLock<ReplicaState>>,
    stop_rx: &mut broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    // Optional AUTH on the repl channel (masterauth carry-forward from T4).
    if let Some(pass) = masterauth {
        stream.write_all(&encode_auth(pass)).await?;
        stream.flush().await?;
        let resp = read_resp_line(&mut stream).await?;
        if !resp.starts_with('+') {
            anyhow::bail!("master repl AUTH rejected: {resp}");
        }
    }
    // Handshake: RESP REPLICAOF sync (master also accepts raw text).
    stream
        .write_all(b"*2\r\n$9\r\nREPLICAOF\r\n$4\r\nsync\r\n")
        .await?;
    stream.flush().await?;

    let mut buf = BytesMut::with_capacity(8 * 1024);
    let mut tmp = [0u8; 4096];

    // Read FULLRESYNC frame.
    let full = loop {
        let mut probe = buf.clone();
        match decode_frames(&mut probe) {
            Ok(frames) => {
                if let Some(f) = frames
                    .into_iter()
                    .find(|f| matches!(f, ReplFrame::Fullresync { .. }))
                {
                    // Consume decoded frames from `buf` (Fullresync + any
                    // leading frames; stray Commands before fullsync are dropped).
                    let _ = decode_frames(&mut buf);
                    break f;
                }
            }
            Err(_) => anyhow::bail!("bad frame while waiting for FULLRESYNC"),
        }
        let n = read_stream_byte(&mut stream, &mut tmp, repl_timeout_secs, stop_rx).await?;
        if n == 0 {
            anyhow::bail!("eof before FULLRESYNC");
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let (repl_id, offset) = match full {
        ReplFrame::Fullresync { repl_id, offset } => (repl_id, offset),
        _ => unreachable!(),
    };
    tracing::info!("replica fullsync from {addr} repl_id={repl_id} offset={offset}");

    // Snapshot envelope: u32 BE length + raw bytes.
    while buf.len() < 4 {
        let n = read_stream_byte(&mut stream, &mut tmp, repl_timeout_secs, stop_rx).await?;
        if n == 0 {
            anyhow::bail!("eof reading snapshot length");
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let snapshot_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    buf.advance(4);
    while buf.len() < snapshot_len {
        let n = read_stream_byte(&mut stream, &mut tmp, repl_timeout_secs, stop_rx).await?;
        if n == 0 {
            anyhow::bail!("eof reading snapshot body");
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let snapshot = buf.split_to(snapshot_len).to_vec();

    // Fullsync: always flush local first; load RDB bytes when present.
    rt.flush_all_shards();
    if snapshot_len > 0 {
        let n = jx_persist::load_rdb_bytes(rt, &snapshot, None)?;
        tracing::info!("replica loaded {n} snapshot entries from master");
    } else {
        tracing::info!("replica fullsync with empty snapshot (flush only)");
    }
    set_status(shared, "streaming", Some(offset));

    // Incremental stream.
    loop {
        let n = read_stream_byte(&mut stream, &mut tmp, repl_timeout_secs, stop_rx).await?;
        if n == 0 {
            anyhow::bail!("master closed replication stream");
        }
        buf.extend_from_slice(&tmp[..n]);
        match decode_frames(&mut buf) {
            Ok(frames) => {
                for f in frames {
                    match f {
                        ReplFrame::Command(payload) => {
                            if let Some(args) = decode_command_args(&payload) {
                                let _ = rt.exec_no_aof(args);
                            }
                            // Advance master_offset by the teed frame size
                            // (matches master on_write accounting: offset
                            // += encoded frame length). Keeps INFO meaningful
                            // between heartbeats.
                            let frame_len = (5 + payload.len()) as u64;
                            {
                                let mut g = shared.write().unwrap();
                                g.status = "streaming".into();
                                g.master_offset = g.master_offset.saturating_add(frame_len);
                            }
                        }
                        ReplFrame::Heartbeat(off) => {
                            set_status(shared, "streaming", Some(off));
                        }
                        ReplFrame::Fullresync { repl_id, offset } => {
                            // Mid-stream FULLRESYNC: do not flush-and-stream
                            // without a snapshot. Drop the connection so
                            // sync_loop reconnects and re-FULLRESYNCs properly.
                            anyhow::bail!(
                                "mid-stream FULLRESYNC repl_id={repl_id} offset={offset}; reconnecting for fullsync"
                            );
                        }
                    }
                }
            }
            Err(_) => anyhow::bail!("bad replication frame in stream"),
        }
    }
}

fn encode_auth(password: &str) -> Vec<u8> {
    jx_persist::encode_command(&[
        Bytes::from_static(b"AUTH"),
        Bytes::copy_from_slice(password.as_bytes()),
    ])
}

async fn read_resp_line(stream: &mut TcpStream) -> anyhow::Result<String> {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            anyhow::bail!("eof reading repl AUTH response");
        }
        out.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
        if out.len() > 512 {
            anyhow::bail!("repl AUTH response too long");
        }
    }
    Ok(String::from_utf8_lossy(&out).to_string())
}

/// Decode a `Command` frame payload (`jx_persist::encode_command` RESP array).
fn decode_command_args(payload: &[u8]) -> Option<Vec<Bytes>> {
    let mut dec = RespDecoder::new();
    dec.feed(payload);
    match dec.next_value().ok().flatten()? {
        RespValue::Array(Some(items)) => {
            let args: Vec<Bytes> = items
                .into_iter()
                .filter_map(|x| match x {
                    RespValue::BulkString(Some(b)) => Some(b),
                    _ => None,
                })
                .collect();
            if args.is_empty() {
                None
            } else {
                Some(args)
            }
        }
        _ => None,
    }
}
