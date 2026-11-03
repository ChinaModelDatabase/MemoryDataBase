use crate::config::{ConfigRuntime, Role, TlsMode};
use crate::dispatch;
use crate::persistence_cmd::{self, PersistenceCtx};
use crate::repl::{self, ReplHandle};
use anyhow::Result;
use bytes::Bytes;
use jx_engine::actor::ShardRuntime;
use jx_proto::{encode, RespDecoder, RespValue};
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// `repl`: shared MasterRepl/ReplicaHandle state for INFO replication fill
/// and REPLICAOF role switching (tee uses `repl.master` when present).
pub async fn handle_conn<S>(
    mut stream: S,
    rt: Arc<ShardRuntime>,
    runtime: Arc<RwLock<ConfigRuntime>>,
    pctx: Option<PersistenceCtx>,
    repl: ReplHandle,
    max_pipeline: usize,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let max_pipeline = max_pipeline.max(1);
    let mut dec = RespDecoder::new();
    let mut buf = vec![0u8; 16 * 1024];
    // Auth gate uses the live password from ConfigRuntime.
    let mut authed = runtime.read().unwrap().requirepass_sm3.is_none();
    // Set when a client negotiates RESP3 via `HELLO 3`; replies are then
    // reshaped to RESP3 (null `_`, maps, WITHSCORES pairs) for that
    // connection only.
    let mut proto3 = false;
    // Reused across reads: pipeline responses encode into one buffer, one write.
    let mut out = bytes::BytesMut::with_capacity(8 * 1024);
    loop {
        let n = match stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        dec.feed(&buf[..n]);
        // Drain complete commands (cap max_pipeline), execute in order,
        // encode all responses into `out`, write once. A partial trailing
        // command stays in the decoder for the next read.
        loop {
            let mut cmds: Vec<Vec<Bytes>> = Vec::new();
            let mut decode_err = false;
            while cmds.len() < max_pipeline {
                match dec.next_value() {
                    Ok(None) => break,
                    Ok(Some(v)) => cmds.push(extract_args(v)),
                    Err(_) => {
                        decode_err = true;
                        break;
                    }
                }
            }
            let mut quit = false;
            let drained = cmds.len();
            if drained > 0 {
                out.clear();
                for args in cmds {
                    let (resp, q) =
                        run_command(args.clone(), &rt, &runtime, &pctx, &repl, &mut authed).await;
                    if matches!(resp, RespValue::Map(_)) {
                        proto3 = true;
                    }
                    let resp = if proto3 { to_resp3(&args, resp) } else { resp };
                    if matches!(resp, RespValue::Error(_)) {
                        jx_engine::stats::inc(&jx_engine::stats::STATS.cmd_errors);
                    } else {
                        jx_engine::stats::inc(&jx_engine::stats::STATS.cmd_processed);
                    }
                    // Replication tee (master role): successful writes enter
                    // the backlog and fan out to replicas. `on_write` filters
                    // by is_write_cmd; Error responses are not teed.
                    if let Some(m) = repl.master() {
                        if !matches!(resp, RespValue::Error(_)) {
                            m.on_write(&args);
                        }
                    }
                    // Backpressure (I1): "ERR shard command queue full" means
                    // the shard actor is saturated. Pause ~1ms so the actor can
                    // drain before the next command in this pipeline (and before
                    // more bytes are read). Bounds client spin under overload
                    // without blocking the bounded shard channel itself.
                    if is_queue_full_err(&resp) {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                    encode(&resp, &mut out);
                    if q {
                        quit = true;
                        break;
                    }
                }
                if !out.is_empty() {
                    stream.write_all(&out).await?;
                    stream.flush().await?;
                }
            }
            // QUIT ends the connection after this batch is written; a decode
            // error is a protocol failure — close after flushing what we have.
            if quit || decode_err {
                return Ok(());
            }
            // Full batch: decoder may still hold complete commands — keep
            // draining without a new read. Otherwise wait for more bytes.
            if drained < max_pipeline {
                break;
            }
        }
    }
    Ok(())
}

/// Reshape a RESP2 reply into the RESP3 form clients expect once they
/// negotiated protocol 3 (otherwise default modern clients misparse nulls,
/// maps and WITHSCORES pairs).
fn to_resp3(args: &[Bytes], resp: RespValue) -> RespValue {
    fn nulls(v: RespValue) -> RespValue {
        match v {
            RespValue::BulkString(None) => RespValue::null(),
            RespValue::Array(Some(items)) => {
                RespValue::array(items.into_iter().map(nulls).collect())
            }
            RespValue::Map(entries) => RespValue::map(
                entries
                    .into_iter()
                    .map(|(k, v)| (nulls(k), nulls(v)))
                    .collect(),
            ),
            other => other,
        }
    }
    fn pairs(v: RespValue) -> RespValue {
        match v {
            RespValue::Array(Some(items)) => RespValue::array(
                items
                    .chunks(2)
                    .map(|c| {
                        RespValue::array(vec![
                            c[0].clone(),
                            c.get(1).cloned().unwrap_or(RespValue::null()),
                        ])
                    })
                    .collect(),
            ),
            other => other,
        }
    }
    fn map_of(v: RespValue) -> RespValue {
        match v {
            RespValue::Array(Some(items)) => RespValue::map(
                items
                    .chunks(2)
                    .map(|c| (c[0].clone(), c.get(1).cloned().unwrap_or(RespValue::null())))
                    .collect(),
            ),
            other => other,
        }
    }
    let name = args
        .first()
        .map(|n| String::from_utf8_lossy(n).to_ascii_uppercase())
        .unwrap_or_default();
    let v = nulls(resp);
    match name.as_str() {
        "HGETALL" => map_of(v),
        "CONFIG"
            if args
                .get(1)
                .map(|s| s.as_ref().eq_ignore_ascii_case(b"GET"))
                .unwrap_or(false) =>
        {
            map_of(v)
        }
        "ZRANGE" | "ZRANGEBYSCORE"
            if args
                .iter()
                .any(|a| a.as_ref().eq_ignore_ascii_case(b"WITHSCORES")) =>
        {
            pairs(v)
        }
        _ => v,
    }
}

fn extract_args(v: RespValue) -> Vec<Bytes> {
    match v {
        RespValue::Array(Some(items)) => items
            .into_iter()
            .filter_map(|x| match x {
                RespValue::BulkString(Some(b)) => Some(b),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

/// True for the engine's retryable overload signal (`ERR shard command queue
/// full`). The conn layer pauses ~1ms after seeing it so the shard actor can
/// drain (I1 backpressure).
fn is_queue_full_err(resp: &RespValue) -> bool {
    matches!(resp, RespValue::Error(msg) if msg.contains("shard command queue full"))
}

fn slowlog_entry_to_resp(e: &crate::slowlog::SlowlogEntry) -> RespValue {
    RespValue::array(vec![
        RespValue::int(e.id as i64),
        RespValue::int(e.timestamp_us),
        RespValue::int(e.duration_us),
        RespValue::array(e.args.iter().cloned().map(RespValue::bulk).collect()),
    ])
}

/// Run one command through the existing intercept path (AUTH gate, CONFIG,
/// persistence, REPLICAOF, INFO replication). Returns `(response, quit)` —
/// `quit` is true for QUIT, whose `+OK` must still be written with the rest
/// of the pipeline batch.
async fn run_command(
    args: Vec<Bytes>,
    rt: &Arc<ShardRuntime>,
    runtime: &Arc<RwLock<ConfigRuntime>>,
    pctx: &Option<PersistenceCtx>,
    repl: &ReplHandle,
    authed: &mut bool,
) -> (RespValue, bool) {
    let start = std::time::Instant::now();
    let slow_args = args.clone();
    if args.is_empty() {
        return (RespValue::err("ERR invalid command"), false);
    }
    let name = std::str::from_utf8(&args[0])
        .unwrap_or("")
        .to_ascii_uppercase();
    if name == "AUTH" {
        let expected = runtime.read().unwrap().requirepass_sm3.clone();
        let ok = match (args.get(1), expected.as_ref()) {
            (Some(p), Some(h)) => {
                let pass = String::from_utf8_lossy(p.as_ref());
                verify_requirepass(h, &pass)
            }
            (Some(_), None) => true,
            _ => false,
        };
        *authed = ok;
        let resp = if ok {
            RespValue::ok()
        } else {
            jx_engine::stats::inc(&jx_engine::stats::STATS.rejected_connections);
            RespValue::err("WRONGPASS invalid username-password pair")
        };
        return (resp, false);
    }
    if name == "HELLO" {
        return (hello_command(&args, runtime, authed), false);
    }
    if !*authed {
        return (RespValue::err("NOAUTH Authentication required."), false);
    }
    if name == "QUIT" {
        return (RespValue::ok(), true);
    }
    // Replica role: client writes are rejected. Reads and non-write commands
    // (PING/CONFIG/INFO/REPLICAOF/...) stay allowed. The replication channel
    // bypasses this gate via `ShardRuntime::exec_no_aof`.
    if runtime.read().unwrap().role == Role::Replica && repl::is_write_cmd(&args) {
        return (
            RespValue::err("READONLY You can't write against a read only replica."),
            false,
        );
    }
    if name == "CLIENT" {
        let sub = args
            .get(1)
            .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase())
            .unwrap_or_default();
        return match sub.as_str() {
            "LIST" => (
                RespValue::bulk(Bytes::from_static(b"addr=127.0.0.1:6379\r\n")),
                false,
            ),
            "KILL" => (RespValue::ok(), false),
            _ => (RespValue::err("ERR syntax error"), false),
        };
    }
    if name == "SLOWLOG" {
        let sub = args
            .get(1)
            .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase())
            .unwrap_or_default();
        let sl = runtime.read().unwrap().slowlog.clone();
        return match sub.as_str() {
            "LEN" => (RespValue::int(sl.len() as i64), false),
            "RESET" => {
                sl.reset();
                (RespValue::ok(), false)
            }
            "GET" => {
                let n = args
                    .get(2)
                    .and_then(|b| String::from_utf8_lossy(b).parse::<usize>().ok());
                let entries = sl.get(n);
                (
                    RespValue::array(entries.iter().map(slowlog_entry_to_resp).collect()),
                    false,
                )
            }
            _ => (RespValue::err("ERR syntax error"), false),
        };
    }
    let resp = if name == "CONFIG" {
        config_command(rt, runtime, &args)
    } else if name == "REPLICAOF" {
        replicaof_command(rt, runtime, repl, &args)
    } else if name == "INFO" {
        let base = dispatch::dispatch(rt, args.clone());
        let full = append_replication_info(base, runtime, repl);
        finish_info(full, &args, rt, runtime, pctx)
    } else if let Some(ctx) = pctx {
        // SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF are intercepted
        // here (conn layer) so the engine stays persist-free.
        // Blocking I/O runs on tokio's blocking pool.
        if matches!(
            name.as_str(),
            "SAVE" | "BGSAVE" | "LASTSAVE" | "BGREWRITEAOF"
        ) {
            let ctx = ctx.clone();
            let name_owned = name.clone();
            match tokio::task::spawn_blocking(move || {
                persistence_cmd::try_handle(&name_owned, &ctx, &[])
            })
            .await
            {
                Ok(Some(r)) => r,
                Ok(None) => dispatch::dispatch(rt, args),
                Err(_) => RespValue::err("ERR persistence task failed"),
            }
        } else {
            match persistence_cmd::try_handle(&name, ctx, &args) {
                Some(resp) => resp,
                None => dispatch::dispatch(rt, args),
            }
        }
    } else {
        dispatch::dispatch(rt, args)
    };
    if name != "AUTH" && name != "QUIT" && name != "CLIENT" && name != "SLOWLOG" {
        let elapsed = start.elapsed().as_micros() as i64;
        let handle = runtime.read().unwrap().slowlog.clone();
        handle.record(&slow_args, elapsed);
    }
    (resp, false)
}

/// RESP2 `HELLO [protover] [AUTH user pass] [SETNAME name]`.
///
/// Clients (redis-py, RESP handshake) open with HELLO; refusing it makes
/// mainstream clients unusable. Only protocol version 2 is offered, so the
/// reply stays a RESP2 map (flat array of alternating field/value). HELLO is
/// allowed before AUTH so `HELLO ... AUTH ...` can authenticate.
fn hello_command(
    args: &[Bytes],
    runtime: &Arc<RwLock<ConfigRuntime>>,
    authed: &mut bool,
) -> RespValue {
    let mut proto = 2i64;
    let mut i = 1;
    if let Some(first) = args.get(1) {
        if let Ok(v) = String::from_utf8_lossy(first).parse::<i64>() {
            if v != 2 && v != 3 {
                return RespValue::err("NOPROTO unsupported protocol version");
            }
            proto = v;
            i = 2;
        }
    }
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        match opt.as_str() {
            "AUTH" => {
                let Some(pass) = args.get(i + 2) else {
                    return RespValue::err("ERR syntax error");
                };
                let expected = runtime.read().unwrap().requirepass_sm3.clone();
                let Some(expected) = expected else {
                    return RespValue::err("ERR Client sent AUTH, but no password is set.");
                };
                let pass = String::from_utf8_lossy(pass);
                if !verify_requirepass(&expected, &pass) {
                    jx_engine::stats::inc(&jx_engine::stats::STATS.rejected_connections);
                    return RespValue::err("WRONGPASS invalid username-password pair");
                }
                *authed = true;
                i += 3;
            }
            "SETNAME" => {
                if args.get(i + 1).is_none() {
                    return RespValue::err("ERR syntax error");
                }
                i += 2;
            }
            _ => return RespValue::err("ERR syntax error"),
        }
    }
    let role = match runtime.read().unwrap().role {
        Role::Replica => "replica",
        _ => "master",
    };
    // RESP3 is accepted so default modern clients (redis-py >= 5 sends
    // `HELLO 3`) can connect; the map is emitted in RESP3 form but the
    // connection keeps speaking RESP2 types afterwards.
    let fields: Vec<(Bytes, RespValue)> = vec![
        (
            Bytes::from_static(b"server"),
            RespValue::bulk(Bytes::from_static(b"MemoryDataBase")),
        ),
        (
            Bytes::from_static(b"version"),
            RespValue::bulk(Bytes::from_static(b"0.1.0")),
        ),
        (Bytes::from_static(b"proto"), RespValue::int(proto)),
        (Bytes::from_static(b"id"), RespValue::int(next_conn_id())),
        (
            Bytes::from_static(b"mode"),
            RespValue::bulk(Bytes::from_static(b"standalone")),
        ),
        (
            Bytes::from_static(b"role"),
            RespValue::bulk(Bytes::from(role.as_bytes().to_vec())),
        ),
        (Bytes::from_static(b"modules"), RespValue::array(vec![])),
    ];
    if proto == 3 {
        return RespValue::map(
            fields
                .into_iter()
                .map(|(k, v)| (RespValue::bulk(k), v))
                .collect(),
        );
    }
    let mut flat = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields {
        flat.push(RespValue::bulk(k));
        flat.push(v);
    }
    RespValue::array(flat)
}

fn next_conn_id() -> i64 {
    static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Conn-layer `REPLICAOF host port` / `REPLICAOF NO ONE`.
///
/// Handled here (not ShardState): switches the shared `ConfigRuntime.role`
/// view and manages the background replica task via `ReplHandle`. Standalone
/// accepts both forms (in-memory role switch).
fn replicaof_command(
    rt: &Arc<ShardRuntime>,
    runtime: &Arc<RwLock<ConfigRuntime>>,
    repl: &ReplHandle,
    args: &[Bytes],
) -> RespValue {
    if args.len() != 3 {
        return RespValue::err("ERR wrong number of arguments for 'replicaof' command");
    }
    let a1 = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    let a2 = String::from_utf8_lossy(&args[2]).to_ascii_uppercase();
    if a1 == "NO" && a2 == "ONE" {
        // Promote: stop replica task, become master, accept writes.
        // Lazy-spawn the master tee when this node never had one (dynamic
        // promotion from replica/standalone), so the promoted node can accept
        // new replicas and tee writes. Repl port follows the configured
        // `repl-port` (default: this node's client port + 1). The listener
        // binds to the node's client `bind` address.
        repl.stop_replica();
        let (repl_port, backlog_size, requirepass, bind) = {
            let mut g = runtime.write().unwrap();
            g.role = Role::Master;
            g.replicaof = None;
            (
                g.repl_port,
                g.repl_backlog_size,
                g.requirepass_sm3.clone(),
                g.bind.clone(),
            )
        };
        if repl.master().is_none() {
            let m = crate::repl::master::spawn_master_repl(
                rt.clone(),
                &bind,
                repl_port,
                backlog_size,
                jx_engine::system_now_ms,
                requirepass,
            );
            repl.set_master(m);
        }
        return RespValue::ok();
    }
    let host = String::from_utf8_lossy(&args[1]).to_string();
    let port: u16 = match String::from_utf8_lossy(&args[2]).parse() {
        Ok(p) if p != 0 => p,
        _ => return RespValue::err("ERR Port is not an integer or out of range"),
    };
    // Demote teardown: if this node was Master, stop the master repl
    // listener + heartbeat tasks and clear `repl.master` (no lingering
    // FULLRESYNC source; writes are no longer teed).
    repl.clear_master();
    // `replicaof host port` is the master client port; repl port = port + 1
    // (same convention as Config::repl_port / net.rs startup).
    let master_repl_port = port.saturating_add(1);
    // Carry `masterauth` / `repl-timeout` from runtime config into the
    // dynamic spawn.
    let (masterauth, repl_timeout_secs) = {
        let g = runtime.read().unwrap();
        (g.masterauth.clone(), g.repl_timeout_secs)
    };
    let handle = repl::spawn_replica(
        rt.clone(),
        host.clone(),
        master_repl_port,
        masterauth,
        repl_timeout_secs,
    );
    repl.set_replica(handle);
    let mut g = runtime.write().unwrap();
    g.role = Role::Replica;
    g.replicaof = Some((host, port));
    RespValue::ok()
}

/// Complete an INFO reply: fill the memory limit fields, add the
/// persistence/security sections, and honour `INFO <section>` selection.
fn finish_info(
    resp: RespValue,
    args: &[Bytes],
    rt: &Arc<ShardRuntime>,
    runtime: &Arc<RwLock<ConfigRuntime>>,
    pctx: &Option<PersistenceCtx>,
) -> RespValue {
    let RespValue::BulkString(Some(text)) = resp else {
        return resp;
    };
    let body = String::from_utf8_lossy(&text).to_string();
    // (lowercase key for lookup, header as emitted, lines)
    let mut sections: Vec<(String, String, Vec<String>)> = Vec::new();
    for line in body.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(name) = line.strip_prefix('#') {
            let name = name.trim();
            sections.push((name.to_ascii_lowercase(), name.to_string(), Vec::new()));
        } else if !line.is_empty() {
            if let Some(last) = sections.last_mut() {
                last.2.push(line.to_string());
            }
        }
    }
    let (maxmemory, policy) = jx_engine::coordinator::memory_limits(rt);
    if let Some(mem) = sections.iter_mut().find(|(n, _, _)| n == "memory") {
        mem.2.push(format!("maxmemory:{maxmemory}"));
        mem.2.push(format!("maxmemory_policy:{policy}"));
    }
    let (appendonly, requirepass_set, tls_mode) = {
        let g = runtime.read().unwrap();
        (g.appendonly, g.requirepass_sm3.is_some(), g.tls_mode)
    };
    sections.push((
        "persistence".to_string(),
        "Persistence".to_string(),
        jx_engine::stats::info_persistence_section(appendonly, pctx.is_some()),
    ));
    sections.push((
        "security".to_string(),
        "Security".to_string(),
        vec![
            format!("tls_mode:{}", tls_mode_name(tls_mode)),
            format!("requirepass_set:{}", u8::from(requirepass_set)),
        ],
    ));

    let want = args
        .get(1)
        .map(|s| String::from_utf8_lossy(s).trim().to_ascii_lowercase());
    let keep: Vec<(String, String, Vec<String>)> = match want.as_deref() {
        None | Some("") | Some("all") | Some("default") | Some("everything") => sections,
        Some(name) => sections.into_iter().filter(|(n, _, _)| n == name).collect(),
    };
    let mut out = String::new();
    for (_, header, lines) in keep {
        out.push_str(&format!("# {header}\r\n"));
        for line in lines {
            out.push_str(&line);
            out.push_str("\r\n");
        }
    }
    RespValue::bulk(Bytes::from(out))
}

fn tls_mode_name(mode: TlsMode) -> &'static str {
    match mode {
        TlsMode::Plain => "plain",
        TlsMode::Tls => "tls",
        TlsMode::Tlcp => "tlcp",
    }
}

/// Append the `# Replication` section to engine INFO text.
fn append_replication_info(
    resp: RespValue,
    runtime: &Arc<RwLock<ConfigRuntime>>,
    repl: &ReplHandle,
) -> RespValue {
    let RespValue::BulkString(Some(text)) = resp else {
        return resp;
    };
    let (role, replica_state, repl_port) = {
        let g = runtime.read().unwrap();
        let state = repl.replica.lock().unwrap().as_ref().map(|h| h.state());
        (g.role, state, g.repl_port)
    };
    let section =
        repl::info_replication_section(role, repl.master().as_deref(), replica_state, repl_port);
    let mut buf = text.to_vec();
    buf.extend_from_slice(section.as_bytes());
    RespValue::bulk(Bytes::from(buf))
}

/// Verify requirepass-sm3 against a presented password.
///
/// Supported config formats:
/// - unsalted: lowercase hex of `sm3(password)`
/// - salted (preferred): `salthex:hashhex` where salt is 16 bytes and
///   hash = sm3(salt || password) — verified via `jx_crypto::verify_password`.
pub(crate) fn verify_requirepass(config_val: &str, password: &str) -> bool {
    let cfg = config_val.trim();
    if let Some((salt_hex, hash_hex)) = cfg.split_once(':') {
        if !salt_hex.is_empty() && !hash_hex.is_empty() {
            return jx_crypto::verify_password(password, salt_hex, hash_hex);
        }
    }
    let got = hex::encode(jx_crypto::sm3_hash(password.as_bytes()));
    hex_ct_eq(cfg, &got)
}

/// CONFIG surface: engine params (maxmemory, maxmemory-policy) dispatch
/// through the shard runtime; runtime params (loglevel, timeout,
/// appendonly, requirepass-sm3) are read/written on the shared `ConfigRuntime`.
fn config_command(
    rt: &ShardRuntime,
    runtime: &Arc<RwLock<ConfigRuntime>>,
    args: &[Bytes],
) -> RespValue {
    if args.len() < 2 {
        return RespValue::err("ERR wrong number of arguments for 'config' command");
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "GET" => {
            if args.len() != 3 {
                return RespValue::err("ERR wrong number of arguments for 'config|get' command");
            }
            let param = String::from_utf8_lossy(&args[2]).to_ascii_lowercase();
            match param.as_str() {
                "maxmemory" | "maxmemory-policy" => dispatch::dispatch(rt, args.to_vec()),
                "appendonly" => {
                    let v = if runtime.read().unwrap().appendonly {
                        "yes"
                    } else {
                        "no"
                    };
                    config_pair(&param, v)
                }
                "loglevel" => {
                    let v = runtime.read().unwrap().loglevel.clone();
                    config_pair(&param, &v)
                }
                "timeout" => {
                    let v = runtime.read().unwrap().timeout_secs.to_string();
                    config_pair(&param, &v)
                }
                "requirepass-sm3" => {
                    // Never echo the password hash back to clients.
                    let v = if runtime.read().unwrap().requirepass_sm3.is_some() {
                        "(hidden)".to_string()
                    } else {
                        String::new()
                    };
                    config_pair(&param, &v)
                }
                "slowlog-log-slower-than" => {
                    let v = runtime.read().unwrap().slowlog_log_slower_than.to_string();
                    config_pair(&param, &v)
                }
                "slowlog-max-len" => {
                    let v = runtime.read().unwrap().slowlog_max_len.to_string();
                    config_pair(&param, &v)
                }
                other => RespValue::err(format!("ERR unsupported CONFIG parameter '{other}'")),
            }
        }
        "SET" => {
            if args.len() != 4 {
                return RespValue::err("ERR wrong number of arguments for 'config|set' command");
            }
            let param = String::from_utf8_lossy(&args[2]).to_ascii_lowercase();
            let val = String::from_utf8_lossy(&args[3]).to_string();
            match param.as_str() {
                "maxmemory" | "maxmemory-policy" => dispatch::dispatch(rt, args.to_vec()),
                // appendonly is static for MVP; accept SET as no-op.
                "appendonly" => RespValue::ok(),
                "loglevel" => {
                    runtime.write().unwrap().loglevel = val;
                    RespValue::ok()
                }
                "timeout" => match val.parse::<u64>() {
                    Ok(n) => {
                        runtime.write().unwrap().timeout_secs = n;
                        RespValue::ok()
                    }
                    Err(_) => RespValue::err("ERR value is not an integer or out of range"),
                },
                "requirepass-sm3" => {
                    // Empty value disables the password (Redis requirepass "" semantics).
                    let h = if val.is_empty() { None } else { Some(val) };
                    runtime.write().unwrap().requirepass_sm3 = h;
                    RespValue::ok()
                }
                "slowlog-log-slower-than" => match val.parse::<i64>() {
                    Ok(n) => {
                        let (max_len, sl) = {
                            let mut g = runtime.write().unwrap();
                            g.slowlog_log_slower_than = n;
                            (g.slowlog_max_len, g.slowlog.clone())
                        };
                        sl.set_config(crate::slowlog::SlowlogConfig {
                            log_slower_than: n,
                            max_len,
                        });
                        RespValue::ok()
                    }
                    Err(_) => RespValue::err("ERR value is not an integer or out of range"),
                },
                "slowlog-max-len" => match val.parse::<usize>() {
                    Ok(n) => {
                        let (log_slower_than, sl) = {
                            let mut g = runtime.write().unwrap();
                            g.slowlog_max_len = n;
                            (g.slowlog_log_slower_than, g.slowlog.clone())
                        };
                        sl.set_config(crate::slowlog::SlowlogConfig {
                            log_slower_than,
                            max_len: n,
                        });
                        RespValue::ok()
                    }
                    Err(_) => RespValue::err("ERR value is not an integer or out of range"),
                },
                other => RespValue::err(format!("ERR unsupported CONFIG parameter '{other}'")),
            }
        }
        "RESETSTAT" => RespValue::ok(),
        other => RespValue::err(format!("ERR unsupported CONFIG subcommand '{other}'")),
    }
}

fn config_pair(param: &str, val: &str) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(Bytes::copy_from_slice(param.as_bytes())),
        RespValue::bulk(Bytes::copy_from_slice(val.as_bytes())),
    ])
}

fn hex_ct_eq(config_hash: &str, got_hash: &str) -> bool {
    let Ok(a) = hex::decode(config_hash.trim().to_ascii_lowercase()) else {
        return false;
    };
    let Ok(b) = hex::decode(got_hash.trim().to_ascii_lowercase()) else {
        return false;
    };
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}
