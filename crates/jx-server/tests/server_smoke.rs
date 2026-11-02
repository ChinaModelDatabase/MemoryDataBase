use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use jx_engine::actor::spawn_shard_runtime;
use jx_engine::system_now_ms;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-srv-{}-{}-{}", tag, std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn spawn_server(dir: &Path, extra: &str) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let conf = dir.join("jx.conf");
    std::fs::write(
        &conf,
        format!(
            "bind 127.0.0.1\nport {port}\ntls-mode plain\ndir {}\nappendonly no\nshards 2\n{extra}",
            dir.display()
        ),
    )
    .unwrap();

    let conf2 = conf.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cfg = jx_server::config::load_config(&conf2).unwrap();
        rt.block_on(jx_server::run_server(cfg)).ok();
    });

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

#[test]
fn ping_over_tcp() {
    let dir = unique_dir("ping");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(resp, "+PONG\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn set_get_over_tcp() {
    let dir = unique_dir("setget");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = cmd(&mut s, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    assert_eq!(resp, "$3\r\nbar\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn auth_required_over_tcp() {
    let pass = "secret";
    let hash = hex::encode(jx_crypto::sm3_hash(pass.as_bytes()));
    let dir = unique_dir("auth");
    let port = spawn_server(&dir, &format!("requirepass-sm3 {hash}\n"));
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    assert!(resp.starts_with("-NOAUTH"), "got {resp:?}");
    let resp = cmd(
        &mut s,
        format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pass.len(), pass).as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n");
    let resp = cmd(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    assert_eq!(resp, "$-1\r\n");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn restore_rdb_on_startup() {
    let dir = unique_dir("restore");
    let engine = spawn_shard_runtime(2, 1024, system_now_ms);
    engine.exec(vec![
        bytes::Bytes::from_static(b"SET"),
        bytes::Bytes::from_static(b"restored"),
        bytes::Bytes::from_static(b"hello"),
    ]);
    let rdb = dir.join("dump.rdb");
    jx_persist::save_rdb(&engine, &rdb, None).expect("save_rdb");
    assert!(rdb.exists());

    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, b"*2\r\n$3\r\nGET\r\n$8\r\nrestored\r\n");
    assert_eq!(resp, "$5\r\nhello\r\n", "rdb restore failed");
    let _ = std::fs::remove_dir_all(&dir);
}

/// RDB + combined AOF both present with appendonly yes: AOF is the recovery
/// base (Redis-like; matches original design "AOF 优先于 RDB"). RDB keys not
/// present in the AOF stream are NOT loaded — replaying a full-state AOF on
/// top of RDB double-applies non-idempotent ops (C1).
#[test]
fn restore_rdb_then_aof_on_startup() {
    let dir = unique_dir("restore-both");
    // RDB snapshot with key from_rdb.
    let engine = spawn_shard_runtime(2, 1024, system_now_ms);
    engine.exec(vec![
        bytes::Bytes::from_static(b"SET"),
        bytes::Bytes::from_static(b"from_rdb"),
        bytes::Bytes::from_static(b"rdb_val"),
    ]);
    jx_persist::save_rdb(&engine, &dir.join("dump.rdb"), None).unwrap();

    // Combined AOF with key from_aof (appendonly yes so AOF is the base).
    jx_persist::append_aof(
        &dir.join("appendonly.aof"),
        &[
            bytes::Bytes::from_static(b"SET"),
            bytes::Bytes::from_static(b"from_aof"),
            bytes::Bytes::from_static(b"aof_val"),
        ],
        None,
    )
    .unwrap();

    let port = spawn_server(&dir, "appendonly yes\n");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, b"*2\r\n$3\r\nGET\r\n$8\r\nfrom_aof\r\n");
    assert_eq!(
        resp, "$7\r\naof_val\r\n",
        "AOF is recovery base, got {resp:?}"
    );
    let resp = cmd(&mut s, b"*2\r\n$3\r\nGET\r\n$8\r\nfrom_rdb\r\n");
    assert_eq!(
        resp, "$-1\r\n",
        "RDB-only key must not load when combined AOF is base, got {resp:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// tls-mode tls with a bad cert/key must refuse startup (fail-closed), not
/// fall back to plain.
#[test]
fn tls_bad_cert_refuses_startup() {
    let dir = unique_dir("tls-bad");
    std::fs::write(dir.join("bad.crt"), b"not-a-pem").unwrap();
    std::fs::write(dir.join("bad.key"), b"not-a-key").unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let conf = dir.join("jx.conf");
    std::fs::write(
        &conf,
        format!(
            "bind 127.0.0.1\nport {port}\ntls-mode tls\ndir {}\nappendonly no\nshards 1\ntls-cert {}\ntls-key {}\n",
            dir.display(),
            dir.join("bad.crt").display(),
            dir.join("bad.key").display(),
        ),
    )
    .unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cfg = jx_server::config::load_config(&conf).expect("config loads (existence checked only)");
    let result = rt.block_on(jx_server::run_server(cfg));
    let err = result
        .err()
        .expect("tls-mode tls with bad cert must refuse startup");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("tls") || msg.contains("cert") || msg.contains("pem") || msg.contains("key"),
        "error should mention tls/cert failure, got: {msg}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Salted requirepass-sm3 (`salthex:hashhex`) must authenticate via
/// jx_crypto::verify_password.
#[test]
fn salted_requirepass_sm3_auth() {
    let salt = [7u8; 16];
    let salt_hex = hex::encode(salt);
    let hash = jx_crypto::hash_password("s3cret", &salt);
    let val = format!("{salt_hex}:{hash}");
    let dir = unique_dir("salted-auth");
    let port = spawn_server(&dir, &format!("requirepass-sm3 {val}\n"));
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    assert!(resp.starts_with("-NOAUTH"), "got {resp:?}");
    let resp = cmd(&mut s, b"*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n");
    assert_eq!(
        resp, "+OK\r\n",
        "salted AUTH with correct password, got {resp:?}"
    );
    let mut s2 = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s2, b"*2\r\n$4\r\nAUTH\r\n$3\r\nbad\r\n");
    assert!(
        resp.starts_with("-WRONGPASS"),
        "wrong password must fail, got {resp:?}"
    );
    // CONFIG GET redacts the salted value too.
    let mut s3 = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s3, b"*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = cmd(
        &mut s3,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$15\r\nrequirepass-sm3\r\n",
    );
    assert!(
        resp.contains("(hidden)"),
        "salted requirepass must be redacted, got {resp:?}"
    );
    assert!(
        !resp.contains(&salt_hex),
        "must not leak salt, got {resp:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONFIG GET appendonly reports the actual configured value.
#[test]
fn config_get_appendonly_reports_configured_value() {
    let dir = unique_dir("appendonly-cfg");
    let port = spawn_server(&dir, "appendonly yes\n");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n",
    );
    assert!(
        resp.contains("appendonly") && resp.contains("yes"),
        "CONFIG GET appendonly must report yes when configured, got {resp:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
