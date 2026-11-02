use bytes::Bytes;
use jx_engine::actor::spawn_shard_runtime;
use jx_engine::system_now_ms;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-shut-{}-{}-{}", tag, std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_conf(dir: &Path, port: u16, extra: &str) -> std::path::PathBuf {
    let conf = dir.join("jx.conf");
    std::fs::write(
        &conf,
        format!(
            "bind 127.0.0.1\nport {port}\ntls-mode plain\ndir {}\nappendonly no\nshards 2\n{extra}",
            dir.display()
        ),
    )
    .unwrap();
    conf
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Spawn run_server (blocks on shutdown signal in a thread). Returns port.
fn spawn_server(dir: &Path, extra: &str) -> u16 {
    let port = free_port();
    let conf = write_conf(dir, port, extra);
    let conf2 = conf.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cfg = jx_server::config::load_config(&conf2).unwrap();
        rt.block_on(jx_server::run_server(cfg)).ok();
    });
    wait_for_port(port)
}

fn wait_for_port(port: u16) -> u16 {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return port;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not accept connections on port {port}");
}

fn read_resp(s: &mut TcpStream) -> String {
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut buf = vec![0u8; 512];
    let n = s.read(&mut buf).unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

fn cmd(s: &mut TcpStream, raw: &[u8]) -> String {
    s.write_all(raw).unwrap();
    s.flush().unwrap();
    read_resp(s)
}

fn wait_for_file(path: &Path, timeout: Duration) -> bool {
    let start = SystemTime::now();
    while SystemTime::now().duration_since(start).unwrap() < timeout {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    path.exists()
}

fn parse_int_resp(resp: &str) -> i64 {
    resp.trim_start_matches(':')
        .trim_end_matches("\r\n")
        .parse()
        .unwrap_or_else(|_| panic!("expected int RESP, got {resp:?}"))
}

#[test]
fn save_creates_rdb_and_lastsave_advances() {
    let dir = unique_dir("save");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");

    let rdb = dir.join("dump.rdb");
    assert!(!rdb.exists(), "precondition: no dump.rdb yet");

    let resp = cmd(&mut s, b"*1\r\n$8\r\nLASTSAVE\r\n");
    let before = parse_int_resp(&resp);
    assert_eq!(before, 0, "got {resp:?}");

    let resp = cmd(&mut s, b"*1\r\n$4\r\nSAVE\r\n");
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    assert!(rdb.exists(), "SAVE should create dump.rdb");

    let resp = cmd(&mut s, b"*1\r\n$8\r\nLASTSAVE\r\n");
    let after = parse_int_resp(&resp);
    assert!(
        after > before,
        "LASTSAVE should increase after SAVE: {before} -> {after} ({resp:?})"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bgsave_returns_ok_and_file_appears() {
    let dir = unique_dir("bgsave");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");

    let rdb = dir.join("dump.rdb");
    let resp = cmd(&mut s, b"*1\r\n$6\r\nBGSAVE\r\n");
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    assert!(
        wait_for_file(&rdb, Duration::from_secs(2)),
        "BGSAVE should create dump.rdb in background"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bgrewriteaof_returns_ok_and_writes_aof() {
    let dir = unique_dir("bgaof");
    // appendonly is "no" in the conf; BGREWRITEAOF should still write the file.
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");

    let resp = cmd(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");

    let aof = dir.join("appendonly.aof");
    let resp = cmd(&mut s, b"*1\r\n$12\r\nBGREWRITEAOF\r\n");
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    assert!(
        wait_for_file(&aof, Duration::from_secs(2)),
        "BGREWRITEAOF should write appendonly.aof even when appendonly is off"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn shutdown_persist_writes_rdb_after_ops() {
    let dir = unique_dir("shutpersist");
    let port = free_port();
    let conf = write_conf(&dir, port, "");
    let cfg = jx_server::config::load_config(&conf).unwrap();

    // Server thread owns its runtime; external shutdown via broadcast tx.
    let (tx, rx) = broadcast::channel::<()>(1);
    let cfg2 = cfg.clone();
    let slot: Arc<Mutex<Option<jx_server::ServerHandle>>> = Arc::new(Mutex::new(None));
    let slot2 = slot.clone();
    let th = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let h = rt
            .block_on(jx_server::run_server_with_shutdown(cfg2, rx))
            .expect("run_server_with_shutdown");
        *slot2.lock().unwrap() = Some(h);
    });

    wait_for_port(port);
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, b"*3\r\n$3\r\nSET\r\n$8\r\nshutdown\r\n$2\r\nok\r\n");
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    drop(s);

    // Trigger shutdown; accept loop stops and returns ServerHandle.
    tx.send(()).ok();
    drop(tx);
    th.join().expect("server thread");
    let handle = slot.lock().unwrap().take().expect("server handle");

    // Persist as main.rs would after graceful shutdown; shares last_save with SAVE/BGSAVE.
    let pctx = handle.pctx.clone();
    jx_server::shutdown_persist(&pctx, &cfg).expect("shutdown_persist");
    handle.bg.abort();

    let rdb = cfg.dir.join("dump.rdb");
    assert!(rdb.exists(), "shutdown_persist should write dump.rdb");
    assert!(
        pctx.last_save.load(Ordering::SeqCst) > 0,
        "shutdown_persist must update the shared last_save"
    );

    // Reload into a fresh runtime and confirm the key survived.
    let rt2 = spawn_shard_runtime(2, 1024, system_now_ms);
    let n = jx_persist::load_rdb(&rt2, &rdb, None).expect("load_rdb");
    assert_eq!(n, 1, "rdb should contain the SET key");
    let got = rt2.exec(vec![
        Bytes::from_static(b"GET"),
        Bytes::from_static(b"shutdown"),
    ]);
    assert_eq!(got, jx_proto::RespValue::bulk(Bytes::from_static(b"ok")));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn shutdown_persist_appends_and_rewrites_when_appendonly_on() {
    let dir = unique_dir("shut-aof");
    let port = free_port();
    let conf = dir.join("jx.conf");
    std::fs::write(
        &conf,
        format!(
            "bind 127.0.0.1\nport {port}\ntls-mode plain\ndir {}\nappendonly yes\nshards 2\n",
            dir.display()
        ),
    )
    .unwrap();
    let cfg = jx_server::config::load_config(&conf).unwrap();

    let rt = spawn_shard_runtime(2, 1024, system_now_ms);
    rt.exec(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"aofkey"),
        Bytes::from_static(b"aofval"),
    ]);

    let pctx = jx_server::PersistenceCtx::new(Arc::new(rt), cfg.dir.clone(), None);
    jx_server::shutdown_persist(&pctx, &cfg).expect("shutdown_persist");

    let aof = dir.join("appendonly.aof");
    let rdb = dir.join("dump.rdb");
    assert!(aof.exists(), "appendonly yes => rewrite_aof writes aof");
    assert!(rdb.exists(), "shutdown always writes rdb");
    assert!(
        pctx.last_save.load(Ordering::SeqCst) > 0,
        "shutdown_persist must update the shared last_save"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// C1: shutdown with appendonly writes full-state combined AOF + RDB of the
/// same moment. Restart recovery must not double-apply RPUSH on top of RDB.
#[test]
fn shutdown_appendonly_rpush_restart_not_doubled() {
    let dir = unique_dir("shut-rpush");
    let port = free_port();
    let conf = dir.join("jx.conf");
    std::fs::write(
        &conf,
        format!(
            "bind 127.0.0.1\nport {port}\ntls-mode plain\ndir {}\nappendonly yes\nshards 2\n",
            dir.display()
        ),
    )
    .unwrap();
    let cfg = jx_server::config::load_config(&conf).unwrap();

    let (tx, rx) = broadcast::channel::<()>(1);
    let cfg2 = cfg.clone();
    let slot: Arc<Mutex<Option<jx_server::ServerHandle>>> = Arc::new(Mutex::new(None));
    let slot2 = slot.clone();
    let th = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let h = rt
            .block_on(jx_server::run_server_with_shutdown(cfg2, rx))
            .expect("run_server_with_shutdown");
        *slot2.lock().unwrap() = Some(h);
    });

    wait_for_port(port);
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(
        &mut s,
        b"*5\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n",
    );
    assert_eq!(resp, ":3\r\n", "RPUSH 1 2 3 got {resp:?}");
    drop(s);

    tx.send(()).ok();
    drop(tx);
    th.join().expect("server thread");
    let handle = slot.lock().unwrap().take().expect("server handle");
    let pctx = handle.pctx.clone();
    jx_server::shutdown_persist(&pctx, &cfg).expect("shutdown_persist");
    handle.bg.abort();

    let aof = dir.join("appendonly.aof");
    let rdb = dir.join("dump.rdb");
    assert!(
        aof.exists(),
        "appendonly yes => rewrite_aof writes combined aof"
    );
    assert!(rdb.exists(), "shutdown always writes rdb");

    // Restart on the same dir; recovery must use combined AOF as base.
    let port2 = spawn_server(&dir, "appendonly yes\n");
    let mut s2 = TcpStream::connect(("127.0.0.1", port2)).expect("connect");
    let resp = cmd(&mut s2, b"*2\r\n$4\r\nLLEN\r\n$1\r\nl\r\n");
    assert_eq!(
        resp, ":3\r\n",
        "list length must not double after AOF+RDB recovery, got {resp:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
