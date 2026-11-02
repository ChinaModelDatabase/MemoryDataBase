use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir =
        std::env::temp_dir().join(format!("jx-repl-ops-{}-{}-{}", tag, std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
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
    let mut buf = vec![0u8; 8192];
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

fn resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    out
}

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

/// INFO must expose a `# Replication` section with `role:` and
/// `master_repl_offset:` (standalone defaults: role standalone, offset 0).
#[test]
fn info_contains_replication_section() {
    let dir = unique_dir("info-repl");
    let port = free_port();
    let (_p, slot, tx) = spawn_server(&dir, port, "role standalone\n");

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, &resp_cmd(&["INFO"]));
    assert!(
        resp.contains("# Replication\r\n"),
        "INFO must contain # Replication section, got {resp:?}"
    );
    assert!(
        resp.contains("role:standalone"),
        "standalone INFO role must be standalone, got {resp:?}"
    );
    assert!(
        resp.contains("master_repl_offset:"),
        "INFO must contain master_repl_offset:, got {resp:?}"
    );
    assert!(
        resp.contains("connected_slaves:"),
        "INFO must contain connected_slaves:, got {resp:?}"
    );

    stop_server(tx, &slot);
    let _ = std::fs::remove_dir_all(&dir);
}

/// On a replica, writes are READONLY until `REPLICAOF NO ONE` promotes the
/// node; after promotion SET succeeds and INFO reports role:master.
#[test]
fn replicaof_no_one_promotes() {
    let dir = unique_dir("promote");
    let port = free_port();
    let dead = free_port();
    let (_p, slot, tx) = spawn_server(
        &dir,
        port,
        &format!("role replica\nreplicaof 127.0.0.1 {dead}\nbacklog-size 1mb\n"),
    );

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, &resp_cmd(&["SET", "k", "v"]));
    assert!(
        resp.contains("READONLY"),
        "replica SET must be READONLY before promote, got {resp:?}"
    );

    let resp = cmd(&mut s, &resp_cmd(&["INFO"]));
    assert!(
        resp.contains("role:replica"),
        "INFO role must be replica before promote, got {resp:?}"
    );

    let resp = cmd(&mut s, &resp_cmd(&["REPLICAOF", "NO", "ONE"]));
    assert_eq!(resp, "+OK\r\n", "REPLICAOF NO ONE, got {resp:?}");

    let resp = cmd(&mut s, &resp_cmd(&["SET", "k", "v"]));
    assert_eq!(resp, "+OK\r\n", "SET after promote, got {resp:?}");

    let resp = cmd(&mut s, &resp_cmd(&["GET", "k"]));
    assert_eq!(resp, "$1\r\nv\r\n", "GET after promote, got {resp:?}");

    let resp = cmd(&mut s, &resp_cmd(&["INFO"]));
    assert!(
        resp.contains("role:master"),
        "INFO role must be master after promote, got {resp:?}"
    );
    assert!(
        resp.contains("master_repl_offset:"),
        "INFO must still carry master_repl_offset after promote, got {resp:?}"
    );

    stop_server(tx, &slot);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Standalone accepts `REPLICAOF host port` (in-memory role switch) and
/// rejects client writes until `REPLICAOF NO ONE` restores master/standalone
/// write acceptance.
#[test]
fn standalone_replicaof_switches_role() {
    let dir = unique_dir("switch");
    let port = free_port();
    let dead = free_port();
    let (_p, slot, tx) = spawn_server(&dir, port, "role standalone\n");

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(
        &mut s,
        &resp_cmd(&["REPLICAOF", "127.0.0.1", &dead.to_string()]),
    );
    assert_eq!(
        resp, "+OK\r\n",
        "standalone REPLICAOF host port, got {resp:?}"
    );

    let resp = cmd(&mut s, &resp_cmd(&["SET", "a", "1"]));
    assert!(
        resp.contains("READONLY"),
        "SET after standalone→replica switch must be READONLY, got {resp:?}"
    );
    let resp = cmd(&mut s, &resp_cmd(&["INFO"]));
    assert!(
        resp.contains("role:replica"),
        "INFO role must be replica after REPLICAOF host port, got {resp:?}"
    );

    let resp = cmd(&mut s, &resp_cmd(&["REPLICAOF", "NO", "ONE"]));
    assert_eq!(resp, "+OK\r\n", "REPLICAOF NO ONE, got {resp:?}");
    let resp = cmd(&mut s, &resp_cmd(&["SET", "a", "1"]));
    assert_eq!(resp, "+OK\r\n", "SET after NO ONE, got {resp:?}");

    stop_server(tx, &slot);
    let _ = std::fs::remove_dir_all(&dir);
}

/// REPLICAOF is intercepted at the conn layer (never reaches the engine as
/// an unknown command). Wrong arity is a Redis-style error.
#[test]
fn replicaof_bad_arity_errors() {
    let dir = unique_dir("arity");
    let port = free_port();
    let (_p, slot, tx) = spawn_server(&dir, port, "role standalone\n");

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, &resp_cmd(&["REPLICAOF"]));
    assert!(resp.contains("wrong number of arguments"), "got {resp:?}");
    let resp = cmd(&mut s, &resp_cmd(&["REPLICAOF", "NO"]));
    assert!(resp.contains("wrong number of arguments"), "got {resp:?}");
    let resp = cmd(&mut s, &resp_cmd(&["REPLICAOF", "h", "notaport"]));
    assert!(resp.to_ascii_lowercase().contains("port"), "got {resp:?}");

    stop_server(tx, &slot);
    let _ = std::fs::remove_dir_all(&dir);
}
