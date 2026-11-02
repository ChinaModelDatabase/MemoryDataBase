use bytes::Bytes;
use jx_engine::actor::spawn_shard_runtime;
use jx_engine::system_now_ms;
use jx_server::repl::{spawn_replica, Role};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

/// Serialize tests that bind ephemeral ports (free_port race under parallelism).
fn e2e_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-repl-rp-{}-{}-{}", tag, std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Free (p1, p1+1) pair so master's default repl-port = client port + 1.
fn free_port_pair() -> (u16, u16) {
    for _ in 0..200 {
        let p1 = free_port();
        let p2 = p1.saturating_add(1);
        if p2 == 0 {
            continue;
        }
        if std::net::TcpListener::bind(("127.0.0.1", p2)).is_ok() {
            return (p1, p2);
        }
    }
    panic!("no free consecutive port pair");
}

fn write_conf(dir: &Path, body: &str) -> std::path::PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let conf = dir.join("jx.conf");
    std::fs::write(
        &conf,
        format!(
            "bind 127.0.0.1\ntls-mode plain\nappendonly no\nshards 2\ndir {}\n{body}",
            dir.display()
        ),
    )
    .unwrap();
    conf
}

fn wait_for_port(port: u16) {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not accept connections on port {port}");
}

fn read_resp(s: &mut TcpStream) -> String {
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut buf = vec![0u8; 512];
    let n = s
        .read(&mut buf)
        .unwrap_or_else(|e| panic!("read resp: {e:?}"));
    String::from_utf8_lossy(&buf[..n]).to_string()
}

fn cmd(s: &mut TcpStream, raw: &[u8]) -> String {
    s.write_all(raw).unwrap();
    s.flush().unwrap();
    read_resp(s)
}

/// RESP array helper: bulk-encode command args.
fn resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    out
}

/// Spawn run_server on a thread; return (client_port, ServerHandle slot, shutdown tx).
/// Note: ServerHandle is stored only after the server shuts down.
fn spawn_server(
    dir: &Path,
    port: u16,
    body_extra: &str,
) -> (
    u16,
    Arc<Mutex<Option<jx_server::ServerHandle>>>,
    broadcast::Sender<()>,
) {
    let conf = write_conf(dir, &format!("port {port}\n{body_extra}"));
    let (tx, rx) = broadcast::channel::<()>(1);
    let slot: Arc<Mutex<Option<jx_server::ServerHandle>>> = Arc::new(Mutex::new(None));
    let slot2 = slot.clone();
    let conf2 = conf.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cfg = jx_server::config::load_config(&conf2).unwrap();
        let h = rt
            .block_on(jx_server::run_server_with_shutdown(cfg, rx))
            .expect("run_server_with_shutdown");
        *slot2.lock().unwrap() = Some(h);
    });
    wait_for_port(port);
    (port, slot, tx)
}

fn spawn_master_server(
    dir: &Path,
    client_port: u16,
    repl_port: u16,
) -> (
    u16,
    Arc<Mutex<Option<jx_server::ServerHandle>>>,
    broadcast::Sender<()>,
) {
    let out = spawn_server(
        dir,
        client_port,
        &format!("role master\nrepl-port {repl_port}\nbacklog-size 1mb\n"),
    );
    wait_for_port(repl_port);
    out
}

fn spawn_replica_server(
    dir: &Path,
    client_port: u16,
    master_host: &str,
    master_port: u16,
    masterauth: Option<&str>,
) -> (
    u16,
    Arc<Mutex<Option<jx_server::ServerHandle>>>,
    broadcast::Sender<()>,
) {
    let mut extra =
        format!("role replica\nreplicaof {master_host} {master_port}\nbacklog-size 1mb\n");
    if let Some(a) = masterauth {
        extra.push_str(&format!("masterauth {a}\n"));
    }
    spawn_server(dir, client_port, &extra)
}

fn stop_server(tx: broadcast::Sender<()>, slot: &Arc<Mutex<Option<jx_server::ServerHandle>>>) {
    tx.send(()).ok();
    for _ in 0..100 {
        if slot.lock().unwrap().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if let Some(h) = slot.lock().unwrap().take() {
        h.bg.abort();
    }
}

/// Role replica + client write over TCP must return READONLY.
#[test]
fn replica_client_write_returns_readonly() {
    let _guard = e2e_lock();
    let dir = unique_dir("readonly");
    let port = free_port();
    let dead = free_port();
    let (_p, slot, tx) = spawn_replica_server(&dir, port, "127.0.0.1", dead, None);

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, &resp_cmd(&["SET", "k", "v"]));
    assert!(
        resp.contains("READONLY"),
        "replica client write must be READONLY, got {resp:?}"
    );
    assert!(
        resp.contains("You can't write against a read only replica."),
        "READONLY message must match Redis wording, got {resp:?}"
    );

    stop_server(tx, &slot);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Replica role allows client reads; writes/config are not wrongly gated.
#[test]
fn replica_client_reads_allowed() {
    let _guard = e2e_lock();
    let dir = unique_dir("replica-reads");
    let port = free_port();
    let dead = free_port();
    let (_p, slot, tx) = spawn_replica_server(&dir, port, "127.0.0.1", dead, None);

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, &resp_cmd(&["PING"]));
    assert_eq!(
        resp, "+PONG\r\n",
        "replica PING must be allowed, got {resp:?}"
    );
    drop(s);

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, &resp_cmd(&["GET", "k"]));
    assert_eq!(resp, "$-1\r\n", "replica GET must be allowed, got {resp:?}");
    drop(s);

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, &resp_cmd(&["CONFIG", "GET", "loglevel"]));
    assert!(
        !resp.contains("READONLY"),
        "CONFIG must not be READONLY, got {resp:?}"
    );
    assert!(
        resp.contains("loglevel"),
        "CONFIG GET loglevel must work on replica, got {resp:?}"
    );

    stop_server(tx, &slot);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Standalone role still accepts client writes (no READONLY regression).
#[test]
fn standalone_write_not_readonly() {
    let _guard = e2e_lock();
    let dir = unique_dir("standalone-w");
    let port = free_port();
    let (_p, slot, tx) = spawn_server(&dir, port, "role standalone\n");

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, &resp_cmd(&["SET", "k", "v"]));
    assert_eq!(resp, "+OK\r\n", "standalone SET must work, got {resp:?}");

    stop_server(tx, &slot);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Master tee → replica applies Command frames via exec_no_aof.
#[test]
fn replica_applies_master_writes() {
    let _guard = e2e_lock();
    let (m_port, m_repl) = free_port_pair();
    let mdir = unique_dir("master-e2e");
    let (_mp, mslot, mtx) = spawn_master_server(&mdir, m_port, m_repl);

    let engine = Arc::new(spawn_shard_runtime(2, 1024, system_now_ms));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handle = {
        let _g = rt.enter();
        spawn_replica(engine.clone(), "127.0.0.1".into(), m_repl, None, 60)
    };

    let mut applied = false;
    for _ in 0..50 {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "repl", "yes"]));
        assert_eq!(resp, "+OK\r\n", "master SET, got {resp:?}");
        for _ in 0..10 {
            let got = engine.exec(vec![
                Bytes::from_static(b"GET"),
                Bytes::from_static(b"repl"),
            ]);
            if got == jx_proto::RespValue::bulk(Bytes::from_static(b"yes")) {
                applied = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        if applied {
            break;
        }
    }
    assert!(
        applied,
        "replica must apply master write via replication (status={:?})",
        handle.state()
    );
    assert_eq!(handle.state().status, "streaming", "status after sync");

    handle.stop();
    stop_server(mtx, &mslot);
    drop(rt);
    let _ = std::fs::remove_dir_all(&mdir);
}

/// Optional masterauth on the repl handshake: replica connects with AUTH.
#[test]
fn replica_masterauth_handshake() {
    let _guard = e2e_lock();
    let (m_port, m_repl) = free_port_pair();
    let mdir = unique_dir("master-auth");
    let pass = "repl-secret";
    let hash = hex::encode(jx_crypto::sm3_hash(pass.as_bytes()));
    let conf = write_conf(
        &mdir,
        &format!(
            "port {m_port}\nrole master\nrepl-port {m_repl}\nbacklog-size 1mb\nrequirepass-sm3 {hash}\n"
        ),
    );
    let (mtx, mrx) = broadcast::channel::<()>(1);
    let mslot: Arc<Mutex<Option<jx_server::ServerHandle>>> = Arc::new(Mutex::new(None));
    let mslot2 = mslot.clone();
    let conf2 = conf.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cfg = jx_server::config::load_config(&conf2).unwrap();
        let h = rt
            .block_on(jx_server::run_server_with_shutdown(cfg, mrx))
            .expect("master server");
        *mslot2.lock().unwrap() = Some(h);
    });
    wait_for_port(m_port);
    wait_for_port(m_repl);

    let engine = Arc::new(spawn_shard_runtime(2, 1024, system_now_ms));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handle = {
        let _g = rt.enter();
        spawn_replica(
            engine.clone(),
            "127.0.0.1".into(),
            m_repl,
            Some(pass.to_string()),
            60,
        )
    };

    let mut applied = false;
    for _ in 0..50 {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
        let resp = cmd(&mut ms, &resp_cmd(&["AUTH", pass]));
        assert_eq!(resp, "+OK\r\n", "master client AUTH, got {resp:?}");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "auth", "ok"]));
        assert_eq!(resp, "+OK\r\n", "master SET, got {resp:?}");
        for _ in 0..10 {
            let got = engine.exec(vec![
                Bytes::from_static(b"GET"),
                Bytes::from_static(b"auth"),
            ]);
            if got == jx_proto::RespValue::bulk(Bytes::from_static(b"ok")) {
                applied = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        if applied {
            break;
        }
    }
    assert!(
        applied,
        "replica with masterauth must complete handshake and apply writes (status={:?})",
        handle.state()
    );

    // Wrong password must not sync.
    let engine2 = Arc::new(spawn_shard_runtime(2, 1024, system_now_ms));
    let handle2 = {
        let _g = rt.enter();
        spawn_replica(
            engine2.clone(),
            "127.0.0.1".into(),
            m_repl,
            Some("wrong-password".into()),
            60,
        )
    };
    std::thread::sleep(Duration::from_millis(300));
    let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
    let _ = cmd(&mut ms, &resp_cmd(&["AUTH", pass]));
    let _ = cmd(&mut ms, &resp_cmd(&["SET", "wrong", "x"]));
    std::thread::sleep(Duration::from_millis(200));
    let got = engine2.exec(vec![
        Bytes::from_static(b"GET"),
        Bytes::from_static(b"wrong"),
    ]);
    assert_eq!(
        got,
        jx_proto::RespValue::null_bulk(),
        "wrong masterauth must not apply writes"
    );
    handle2.stop();

    handle.stop();
    stop_server(mtx, &mslot);
    drop(rt);
    let _ = std::fs::remove_dir_all(&mdir);
}

/// Full replica server (net.rs path) applies master writes via port+1 convention.
#[test]
fn replica_server_net_path_applies_writes() {
    let _guard = e2e_lock();
    let (m_port, m_repl) = free_port_pair();
    let mdir = unique_dir("master-net");
    let (_mp, mslot, mtx) = spawn_master_server(&mdir, m_port, m_repl);

    let r_port = free_port();
    let rdir = unique_dir("replica-net");
    let conf = write_conf(
        &rdir,
        &format!("port {r_port}\nrole replica\nreplicaof 127.0.0.1 {m_port}\nbacklog-size 1mb\n"),
    );
    let (rtx, rrx) = broadcast::channel::<()>(1);
    let rslot: Arc<Mutex<Option<jx_server::ServerHandle>>> = Arc::new(Mutex::new(None));
    let rslot2 = rslot.clone();
    let conf2 = conf.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cfg = jx_server::config::load_config(&conf2).unwrap();
        let h = rt
            .block_on(jx_server::run_server_with_shutdown(cfg, rrx))
            .expect("replica server");
        *rslot2.lock().unwrap() = Some(h);
    });
    wait_for_port(r_port);

    let mut rs = TcpStream::connect(("127.0.0.1", r_port)).expect("replica client");
    let resp = cmd(&mut rs, &resp_cmd(&["SET", "z", "1"]));
    assert!(resp.contains("READONLY"), "got {resp:?}");

    let mut applied = false;
    for _ in 0..50 {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "net", "v1"]));
        assert_eq!(resp, "+OK\r\n");
        for _ in 0..10 {
            let resp = cmd(&mut rs, &resp_cmd(&["GET", "net"]));
            if resp == "$2\r\nv1\r\n" {
                applied = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        if applied {
            break;
        }
    }
    assert!(applied, "replica server must apply master write over TCP");

    stop_server(rtx, &rslot);
    stop_server(mtx, &mslot);
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

/// Role is exposed on ConfigRuntime for conn-layer READONLY checks.
#[test]
fn config_runtime_carries_role() {
    let _guard = e2e_lock();
    let dir = unique_dir("cfg-role");
    let port = free_port();
    let dead = free_port();
    let conf = write_conf(
        &dir,
        &format!("port {port}\nrole replica\nreplicaof 127.0.0.1 {dead}\n"),
    );
    let cfg = jx_server::config::load_config(&conf).unwrap();
    assert_eq!(cfg.role, Role::Replica);
    let rt = jx_server::ConfigRuntime::from_startup(&cfg);
    assert_eq!(rt.role, Role::Replica);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Wrong masterauth must be rejected on the repl port.
#[test]
fn replica_rejects_wrong_masterauth() {
    let _guard = e2e_lock();
    let (m_port, m_repl) = free_port_pair();
    let mdir = unique_dir("master-badauth");
    let pass = "right-pass";
    let hash = hex::encode(jx_crypto::sm3_hash(pass.as_bytes()));
    let conf = write_conf(
        &mdir,
        &format!(
            "port {m_port}\nrole master\nrepl-port {m_repl}\nbacklog-size 1mb\nrequirepass-sm3 {hash}\n"
        ),
    );
    let (mtx, mrx) = broadcast::channel::<()>(1);
    let mslot: Arc<Mutex<Option<jx_server::ServerHandle>>> = Arc::new(Mutex::new(None));
    let mslot2 = mslot.clone();
    let conf2 = conf.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cfg = jx_server::config::load_config(&conf2).unwrap();
        let h = rt
            .block_on(jx_server::run_server_with_shutdown(cfg, mrx))
            .expect("master server");
        *mslot2.lock().unwrap() = Some(h);
    });
    wait_for_port(m_port);
    wait_for_port(m_repl);

    let engine = Arc::new(spawn_shard_runtime(2, 1024, system_now_ms));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handle = {
        let _g = rt.enter();
        spawn_replica(
            engine.clone(),
            "127.0.0.1".into(),
            m_repl,
            Some("not-the-password".into()),
            60,
        )
    };
    std::thread::sleep(Duration::from_millis(400));
    let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
    let auth = resp_cmd(&["AUTH", pass]);
    let resp = cmd(&mut ms, &auth);
    assert_eq!(resp, "+OK\r\n", "master client AUTH, got {resp:?}");
    let _ = cmd(&mut ms, &resp_cmd(&["SET", "nope", "x"]));
    std::thread::sleep(Duration::from_millis(200));
    let got = engine.exec(vec![
        Bytes::from_static(b"GET"),
        Bytes::from_static(b"nope"),
    ]);
    assert_eq!(
        got,
        jx_proto::RespValue::null_bulk(),
        "wrong masterauth replica must not apply writes"
    );

    handle.stop();
    stop_server(mtx, &mslot);
    drop(rt);
    let _ = std::fs::remove_dir_all(&mdir);
}
