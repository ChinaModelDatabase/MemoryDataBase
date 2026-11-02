//! Dual-node replication e2e: FULLRESYNC (real RDB) + incremental tee,
//! READONLY on replica, `REPLICAOF NO ONE` promotion, and reconnect.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

/// Serialize e2e tests: `free_port`/`free_port_pair` race when parallel tests
/// grab the same ephemeral ports between drop and server bind.
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
    let dir =
        std::env::temp_dir().join(format!("jx-repl-e2e-{}-{}-{}", tag, std::process::id(), n));
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

/// Poll replica GET until it equals `expect` (timeout 5s default).
fn wait_get_eq(port: u16, key: &str, expect: &str, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    let want = format!("${}\r\n{}\r\n", expect.len(), expect);
    let mut last = String::new();
    while Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let resp = cmd(&mut s, &resp_cmd(&["GET", key]));
            last = resp.clone();
            if resp == want {
                return resp;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("replica GET {key} did not converge to {expect:?} within {timeout:?}; last={last:?}");
}

/// T7 e2e: master role on port A, replica role on port B (`replicaof` A);
/// master SET k v; poll replica GET k == v; replica client SET → READONLY;
/// `REPLICAOF NO ONE` on replica then SET succeeds.
#[test]
fn e2e_fullsync_and_incremental() {
    let _guard = e2e_lock();
    let (m_port, m_repl) = free_port_pair();
    let mdir = unique_dir("master");
    let (_mp, mslot, mtx) = spawn_master_server(&mdir, m_port, m_repl);

    let r_port = free_port();
    let rdir = unique_dir("replica");
    let (_rp, rslot, rtx) = spawn_server(
        &rdir,
        r_port,
        &format!("role replica\nreplicaof 127.0.0.1 {m_port}\nbacklog-size 1mb\n"),
    );

    // Incremental: master SET after replica connect must converge on replica.
    {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "k", "v"]));
        assert_eq!(resp, "+OK\r\n", "master SET k v, got {resp:?}");
    }
    wait_get_eq(r_port, "k", "v", Duration::from_secs(5));

    // Replica client writes are READONLY.
    {
        let mut rs = TcpStream::connect(("127.0.0.1", r_port)).expect("replica client");
        let resp = cmd(&mut rs, &resp_cmd(&["SET", "k", "nope"]));
        assert!(
            resp.contains("READONLY"),
            "replica client SET must be READONLY, got {resp:?}"
        );
        // Data untouched by the rejected write.
        let resp = cmd(&mut rs, &resp_cmd(&["GET", "k"]));
        assert_eq!(resp, "$1\r\nv\r\n", "replica GET k after rejected SET");
    }

    // Promote replica via REPLICAOF NO ONE; writes now succeed.
    {
        let mut rs = TcpStream::connect(("127.0.0.1", r_port)).expect("replica client");
        let resp = cmd(&mut rs, &resp_cmd(&["REPLICAOF", "NO", "ONE"]));
        assert_eq!(resp, "+OK\r\n", "REPLICAOF NO ONE, got {resp:?}");
        let resp = cmd(&mut rs, &resp_cmd(&["SET", "promoted", "yes"]));
        assert_eq!(resp, "+OK\r\n", "SET after promote, got {resp:?}");
        let resp = cmd(&mut rs, &resp_cmd(&["GET", "promoted"]));
        assert_eq!(resp, "$3\r\nyes\r\n", "GET promoted after promote");
        let resp = cmd(&mut rs, &resp_cmd(&["INFO"]));
        assert!(
            resp.contains("role:master"),
            "INFO role must be master after promote, got {resp:?}"
        );
    }

    stop_server(rtx, &rslot);
    stop_server(mtx, &mslot);
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

/// FULLRESYNC carries a real RDB snapshot: a key written on the master
/// *before* the replica connects must appear on the replica without any
/// post-connect master write (empty-stub FULLRESYNC would fail this).
#[test]
fn e2e_fullsync_preexisting_snapshot() {
    let _guard = e2e_lock();
    let (m_port, m_repl) = free_port_pair();
    let mdir = unique_dir("master-pre");
    let (_mp, mslot, mtx) = spawn_master_server(&mdir, m_port, m_repl);

    // Pre-existing data on master (before replica ever connects).
    {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "pre", "snapshot"]));
        assert_eq!(resp, "+OK\r\n", "master SET pre, got {resp:?}");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "pre2", "data"]));
        assert_eq!(resp, "+OK\r\n", "master SET pre2, got {resp:?}");
    }

    let r_port = free_port();
    let rdir = unique_dir("replica-pre");
    let (_rp, rslot, rtx) = spawn_server(
        &rdir,
        r_port,
        &format!("role replica\nreplicaof 127.0.0.1 {m_port}\nbacklog-size 1mb\n"),
    );

    // No further master writes: convergence must come from FULLRESYNC RDB.
    wait_get_eq(r_port, "pre", "snapshot", Duration::from_secs(5));
    wait_get_eq(r_port, "pre2", "data", Duration::from_secs(5));

    stop_server(rtx, &rslot);
    stop_server(mtx, &mslot);
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

/// Reconnect: after the master process restarts, the replica's sync task
/// retries (backoff) and re-FULLRESYNCs; new master writes converge again.
#[test]
fn e2e_replica_reconnect_after_master_restart() {
    let _guard = e2e_lock();
    let (m_port, m_repl) = free_port_pair();
    let mdir = unique_dir("master-re");
    let (_mp, mslot, mtx) = spawn_master_server(&mdir, m_port, m_repl);

    let r_port = free_port();
    let rdir = unique_dir("replica-re");
    let (_rp, rslot, rtx) = spawn_server(
        &rdir,
        r_port,
        &format!("role replica\nreplicaof 127.0.0.1 {m_port}\nbacklog-size 1mb\n"),
    );

    {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "before", "1"]));
        assert_eq!(resp, "+OK\r\n", "master SET before, got {resp:?}");
    }
    wait_get_eq(r_port, "before", "1", Duration::from_secs(5));

    // Kill the master (same dir; replica keeps retrying).
    stop_server(mtx, &mslot);
    std::thread::sleep(Duration::from_millis(100));

    // Restart master on the same client + repl ports (replicaof port+1
    // convention unchanged).
    let (_mp2, mslot2, mtx2) = spawn_master_server(&mdir, m_port, m_repl);
    {
        let mut ms =
            TcpStream::connect(("127.0.0.1", m_port)).expect("master client after restart");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "after", "2"]));
        assert_eq!(resp, "+OK\r\n", "master SET after restart, got {resp:?}");
    }
    // Replica must reconnect and apply the new write.
    wait_get_eq(r_port, "after", "2", Duration::from_secs(5));

    stop_server(rtx, &rslot);
    stop_server(mtx2, &mslot2);
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

/// TTL + DEL convergence after fullsync: a key with EXPIRE set on the master
/// (before/after replica connects) must appear on the replica; a subsequent
/// DEL on the master must remove it on the replica.
#[test]
fn e2e_ttl_and_del_converge() {
    let _guard = e2e_lock();
    let (m_port, m_repl) = free_port_pair();
    let mdir = unique_dir("master-ttl");
    let (_mp, mslot, mtx) = spawn_master_server(&mdir, m_port, m_repl);

    // Pre-existing key + TTL before replica connects (fullsync path).
    {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "ttlkey", "v"]));
        assert_eq!(resp, "+OK\r\n", "master SET ttlkey, got {resp:?}");
        let resp = cmd(&mut ms, &resp_cmd(&["EXPIRE", "ttlkey", "100"]));
        assert!(
            resp.contains("1") || resp.contains(":1"),
            "EXPIRE ttlkey, got {resp:?}"
        );
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "delme", "x"]));
        assert_eq!(resp, "+OK\r\n", "master SET delme, got {resp:?}");
    }

    let r_port = free_port();
    let rdir = unique_dir("replica-ttl");
    let (_rp, rslot, rtx) = spawn_server(
        &rdir,
        r_port,
        &format!("role replica\nreplicaof 127.0.0.1 {m_port}\nbacklog-size 1mb\n"),
    );

    wait_get_eq(r_port, "ttlkey", "v", Duration::from_secs(5));
    wait_get_eq(r_port, "delme", "x", Duration::from_secs(5));

    // Incremental DEL on master must converge on replica.
    {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
        let resp = cmd(&mut ms, &resp_cmd(&["DEL", "delme"]));
        assert_eq!(resp, ":1\r\n", "master DEL delme, got {resp:?}");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "post", "y"]));
        assert_eq!(resp, "+OK\r\n", "master SET post, got {resp:?}");
    }
    // delme must disappear; post must appear.
    {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last = String::new();
        let mut del_gone = false;
        let mut post_ok = false;
        while Instant::now() < deadline {
            if let Ok(mut s) = TcpStream::connect(("127.0.0.1", r_port)) {
                let del_resp = cmd(&mut s, &resp_cmd(&["GET", "delme"]));
                let post_resp = cmd(&mut s, &resp_cmd(&["GET", "post"]));
                last = format!("del={del_resp:?} post={post_resp:?}");
                del_gone = del_resp == "$-1\r\n";
                post_ok = post_resp == "$1\r\ny\r\n";
                if del_gone && post_ok {
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(
            del_gone && post_ok,
            "replica must converge DEL (gone) + SET post=y; last={last:?}"
        );
    }

    stop_server(rtx, &rslot);
    stop_server(mtx, &mslot);
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

/// Demote teardown: a node that was Master and issues `REPLICAOF host port`
/// must stop its repl listener (no lingering FULLRESYNC source) and report
/// role:replica.
#[test]
fn e2e_demote_stops_master_repl_listener() {
    let _guard = e2e_lock();
    let (m_port, m_repl) = free_port_pair();
    let mdir = unique_dir("master-demote");
    let (_mp, mslot, mtx) = spawn_master_server(&mdir, m_port, m_repl);
    wait_for_port(m_repl);

    // Demote the master toward a dead peer.
    {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client");
        let dead = free_port();
        let resp = cmd(
            &mut ms,
            &resp_cmd(&["REPLICAOF", "127.0.0.1", &dead.to_string()]),
        );
        assert_eq!(resp, "+OK\r\n", "REPLICAOF demote, got {resp:?}");
        let resp = cmd(&mut ms, &resp_cmd(&["INFO"]));
        assert!(
            resp.contains("role:replica"),
            "INFO role must be replica after demote, got {resp:?}"
        );
    }

    // Repl listener must stop accepting (teardown clears master + shutdown).
    {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut closed = false;
        while Instant::now() < deadline {
            match TcpStream::connect(("127.0.0.1", m_repl)) {
                Ok(mut s) => {
                    // If connect still succeeds, the listener may still be
                    // draining; try a handshake and see if it dies.
                    s.set_read_timeout(Some(Duration::from_millis(300)))
                        .unwrap();
                    let _ = s.write_all(b"*2\r\n$9\r\nREPLICAOF\r\n$4\r\nsync\r\n");
                    let _ = s.flush();
                    let mut buf = [0u8; 64];
                    match s.read(&mut buf) {
                        Ok(0) | Err(_) => {
                            closed = true;
                            break;
                        }
                        Ok(_) => {}
                    }
                }
                Err(_) => {
                    closed = true;
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(
            closed,
            "master repl port {m_repl} must stop accepting after demote (REPLICAOF host port)"
        );
    }

    // Writes are no longer teed as master (role replica → READONLY).
    {
        let mut ms = TcpStream::connect(("127.0.0.1", m_port)).expect("master client after demote");
        let resp = cmd(&mut ms, &resp_cmd(&["SET", "k", "v"]));
        assert!(
            resp.contains("READONLY"),
            "write after demote must be READONLY, got {resp:?}"
        );
    }

    stop_server(mtx, &mslot);
    let _ = std::fs::remove_dir_all(&mdir);
}
