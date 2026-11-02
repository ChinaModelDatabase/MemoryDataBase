use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-ops-{}-{}-{}", tag, std::process::id(), n));
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
    let mut buf = vec![0u8; 4096];
    let n = s.read(&mut buf).unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

fn cmd(s: &mut TcpStream, raw: &[u8]) -> String {
    s.write_all(raw).unwrap();
    s.flush().unwrap();
    read_resp(s)
}

#[test]
fn slowlog_len_reset_get_over_tcp() {
    let dir = unique_dir("slowlog");
    // Threshold of 1us: every executed command qualifies as slow.
    let port = spawn_server(&dir, "slowlog-log-slower-than 1\nslowlog-max-len 8\n");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");

    let resp = cmd(&mut s, b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n");
    let before: i64 = resp
        .trim_start_matches(':')
        .trim_end_matches("\r\n")
        .parse()
        .expect("LEN is integer");

    let resp = cmd(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = cmd(&mut s, b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n");
    let after: i64 = resp
        .trim_start_matches(':')
        .trim_end_matches("\r\n")
        .parse()
        .expect("LEN is integer");
    assert!(
        after > before,
        "SET should be recorded: {before} -> {after}"
    );

    let resp = cmd(&mut s, b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n");
    assert!(resp.starts_with('*'), "GET returns array, got {resp:?}");

    let resp = cmd(&mut s, b"*2\r\n$7\r\nSLOWLOG\r\n$5\r\nRESET\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = cmd(&mut s, b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n");
    assert_eq!(resp, ":0\r\n", "RESET clears entries, got {resp:?}");

    let resp = cmd(&mut s, b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nBAD\r\n");
    assert!(
        resp.starts_with("-ERR"),
        "bad subcommand errors, got {resp:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn client_list_kill_over_tcp() {
    let dir = unique_dir("client");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");

    let resp = cmd(&mut s, b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n");
    assert!(
        resp.starts_with('$'),
        "LIST returns bulk string, got {resp:?}"
    );
    assert!(resp.contains("addr="), "LIST contains addr, got {resp:?}");

    let resp = cmd(&mut s, b"*2\r\n$6\r\nCLIENT\r\n$4\r\nKILL\r\n");
    assert_eq!(resp, "+OK\r\n");

    let resp = cmd(&mut s, b"*2\r\n$6\r\nCLIENT\r\n$3\r\nBAD\r\n");
    assert!(
        resp.starts_with("-ERR"),
        "bad subcommand errors, got {resp:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn info_contains_stats_section() {
    let dir = unique_dir("info");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");

    let resp = cmd(&mut s, b"*1\r\n$4\r\nINFO\r\n");
    assert!(resp.starts_with('$'), "INFO returns bulk, got {resp:?}");
    for field in [
        "# Stats",
        "expired_keys:",
        "evicted_keys:",
        "rejected_connections:",
        "queue_full:",
        "shard_queue_depth_max:",
        "aof_flushes:",
        "rdb_saves:",
        "total_commands_processed:",
    ] {
        assert!(
            resp.contains(field),
            "INFO stats must include {field}, got {resp:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn error_semantics_unknown_and_unsupported_commands() {
    let dir = unique_dir("errs");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");

    // Unknown command
    let resp = cmd(&mut s, b"*1\r\n$7\r\nNOTREAL\r\n");
    assert!(
        resp.starts_with("-ERR unknown command"),
        "unknown command error, got {resp:?}"
    );
    // Known-but-unsupported command returns explicit error (not silent)
    let resp = cmd(&mut s, b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\na\r\n");
    assert!(
        resp.starts_with("-ERR command"),
        "unsupported command explicit error, got {resp:?}"
    );
    // Empty command array
    let resp = cmd(&mut s, b"*0\r\n");
    assert!(
        resp.starts_with("-ERR"),
        "empty command errors, got {resp:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
