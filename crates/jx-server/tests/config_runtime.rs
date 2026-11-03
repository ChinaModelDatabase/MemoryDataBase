use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Duration;

fn unique_dir(tag: &str) -> PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-cfg-rt-{}-{}-{}", tag, std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn spawn_server(dir: &std::path::Path, extra: &str) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let conf = dir.join("jx.conf");
    std::fs::write(
        &conf,
        format!(
            "bind 127.0.0.1\nport {port}\ntls-mode plain\ndir {}\nappendonly no\nshards 2\nmaxmemory 0\nslowlog-log-slower-than 1000\nslowlog-max-len 16\n{extra}",
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

fn resp(port: u16, args: &[&str]) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend(format!("${}\r\n{}\r\n", a.len(), a).into_bytes());
    }
    s.write_all(&out).unwrap();
    s.flush().unwrap();
    let mut buf = vec![0u8; 4096];
    let n = s.read(&mut buf).expect("read reply");
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[test]
fn config_get_slowlog_params_reports_configured_values() {
    let dir = unique_dir("slowlog-get");
    let port = spawn_server(&dir, "");
    let reply = resp(port, &["CONFIG", "GET", "slowlog-log-slower-than"]);
    assert!(
        reply.contains("slowlog-log-slower-than") && reply.contains("1000"),
        "CONFIG GET slowlog-log-slower-than must report 1000, got {reply}"
    );
    let reply = resp(port, &["CONFIG", "GET", "slowlog-max-len"]);
    assert!(
        reply.contains("slowlog-max-len") && reply.contains("16"),
        "CONFIG GET slowlog-max-len must report 16, got {reply}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn config_set_slowlog_params_takes_effect() {
    let dir = unique_dir("slowlog-set");
    let port = spawn_server(&dir, "");
    assert_eq!(
        resp(port, &["CONFIG", "SET", "slowlog-log-slower-than", "0"]),
        "+OK\r\n"
    );
    assert_eq!(
        resp(port, &["CONFIG", "SET", "slowlog-max-len", "4"]),
        "+OK\r\n"
    );

    let reply = resp(port, &["CONFIG", "GET", "slowlog-max-len"]);
    assert!(
        reply.contains("4"),
        "max-len must be 4 after SET, got {reply}"
    );

    // threshold 0 logs every command; the ring must honour max-len = 4.
    for _ in 0..6 {
        assert_eq!(resp(port, &["PING"]), "+PONG\r\n");
    }
    let len_reply = resp(port, &["SLOWLOG", "LEN"]);
    assert_eq!(
        len_reply, ":4\r\n",
        "slowlog must cap at max-len=4, got {len_reply}"
    );

    // Invalid values are rejected instead of silently ignored.
    assert_eq!(
        resp(port, &["CONFIG", "SET", "slowlog-max-len", "abc"]),
        "-ERR value is not an integer or out of range\r\n"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
