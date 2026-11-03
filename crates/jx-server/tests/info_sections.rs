use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn unique_dir(tag: &str) -> PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-info-{}-{}-{}", tag, std::process::id(), n));
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
            "bind 127.0.0.1\nport {port}\ntls-mode plain\ndir {}\nappendonly no\nshards 2\nmaxmemory 64mb\nmaxmemory-policy volatile-lru\n{extra}",
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

fn raw_cmd(port: u16, args: &[&str]) -> String {
    raw_cmd_auth(port, None, args)
}

fn raw_cmd_auth(port: u16, pass: Option<&str>, args: &[&str]) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    if let Some(p) = pass {
        s.write_all(format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", p.len(), p).as_bytes())
            .unwrap();
        s.flush().unwrap();
        let mut abuf = vec![0u8; 128];
        let n = s.read(&mut abuf).expect("auth reply");
        assert_eq!(&abuf[..n], b"+OK\r\n", "AUTH failed");
    }
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend(format!("${}\r\n{}\r\n", a.len(), a).into_bytes());
    }
    s.write_all(&out).unwrap();
    s.flush().unwrap();
    let mut buf = vec![0u8; 16384];
    let n = s.read(&mut buf).expect("read reply");
    let text = String::from_utf8_lossy(&buf[..n]).to_string();
    // Unwrap the bulk reply: strip "$<len>\r\n" prefix and trailing CRLF.
    let body = match text.find("\r\n") {
        Some(i) => &text[i + 2..],
        None => &text,
    };
    body.trim_end_matches("\r\n").to_string()
}

fn cmd(port: u16, args: &[&str]) -> String {
    raw_cmd(port, args)
}

fn sections(text: &str) -> Vec<String> {
    text.lines()
        .filter(|l| l.starts_with('#'))
        .map(|l| l.trim_start_matches("# ").to_ascii_lowercase())
        .collect()
}

#[test]
fn info_all_returns_every_section() {
    let dir = unique_dir("all");
    let port = spawn_server(&dir, "");
    let text = cmd(port, &["INFO"]);
    for want in ["server", "memory", "keyspace", "stats", "replication"] {
        assert!(
            sections(&text).contains(&want.to_string()),
            "INFO must contain #{want}, got {text}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn info_section_returns_only_that_section() {
    let dir = unique_dir("filter");
    let port = spawn_server(&dir, "");
    let text = cmd(port, &["INFO", "memory"]);
    assert_eq!(
        sections(&text),
        vec!["memory".to_string()],
        "INFO memory must return only the memory section, got {text}"
    );
    let text = cmd(port, &["INFO", "replication"]);
    assert_eq!(
        sections(&text),
        vec!["replication".to_string()],
        "got {text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn info_memory_reports_maxmemory_and_policy() {
    let dir = unique_dir("memfields");
    let port = spawn_server(&dir, "");
    let text = cmd(port, &["INFO", "memory"]);
    assert!(
        text.contains("maxmemory:"),
        "memory section must report maxmemory, got {text}"
    );
    assert!(
        text.contains("maxmemory_policy:volatile-lru"),
        "memory section must report maxmemory_policy, got {text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn info_persistence_section_reports_aof_state() {
    let dir = unique_dir("persist");
    let port = spawn_server(&dir, "appendonly yes\n");
    let text = cmd(port, &["INFO", "persistence"]);
    assert_eq!(
        sections(&text),
        vec!["persistence".to_string()],
        "got {text}"
    );
    assert!(
        text.contains("aof_enabled:1"),
        "persistence section must report aof_enabled, got {text}"
    );
    assert!(text.contains("loading:0"), "got {text}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn info_security_section_reports_tls_and_auth() {
    let hash = hex::encode(jx_crypto::sm3_hash(b"secret"));
    let dir = unique_dir("sec");
    let port = spawn_server(&dir, "");
    let text = cmd(port, &["INFO", "security"]);
    assert_eq!(sections(&text), vec!["security".to_string()], "got {text}");
    assert!(
        text.contains("tls_mode:plain"),
        "security section must report tls_mode, got {text}"
    );
    assert!(text.contains("requirepass_set:0"), "got {text}");

    let dir2 = unique_dir("sec-auth");
    let port2 = spawn_server(&dir2, &format!("requirepass-sm3 {hash}\n"));
    let text = raw_cmd_auth(port2, Some("secret"), &["INFO", "security"]);
    assert!(text.contains("requirepass_set:1"), "got {text}");
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
}

#[test]
fn info_unknown_section_returns_empty() {
    let dir = unique_dir("unknown");
    let port = spawn_server(&dir, "");
    let text = cmd(port, &["INFO", "nosuchsection"]);
    assert!(
        text.trim().is_empty(),
        "unknown INFO section must return empty bulk, got {text:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
