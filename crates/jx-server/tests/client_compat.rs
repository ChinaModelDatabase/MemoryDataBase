use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use jx_proto::{RespDecoder, RespValue};

fn unique_dir(tag: &str) -> PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-compat-{}-{}-{}", tag, std::process::id(), n));
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

fn connect(port: u16) -> TcpStream {
    let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    s
}

fn resp(s: &mut TcpStream, raw: &[u8]) -> RespValue {
    s.write_all(raw).unwrap();
    s.flush().unwrap();
    let mut dec = RespDecoder::new();
    loop {
        let mut buf = vec![0u8; 4096];
        let n = s.read(&mut buf).expect("read reply");
        dec.feed(&buf[..n]);
        if let Some(v) = dec.next_value().expect("decode reply") {
            return v;
        }
    }
}

fn arr(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend(format!("${}\r\n{}\r\n", a.len(), a).into_bytes());
    }
    out
}

fn map_get(reply: &RespValue, field: &str) -> Option<RespValue> {
    let RespValue::Array(Some(items)) = reply else {
        return None;
    };
    items
        .windows(2)
        .find(|w| matches!(&w[0], RespValue::BulkString(Some(b)) if b.as_ref() == field.as_bytes()))
        .map(|w| w[1].clone())
}

#[test]
fn hello_returns_resp2_map_with_proto_2() {
    let dir = unique_dir("hello");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    let reply = resp(&mut s, &arr(&["HELLO"]));
    let RespValue::Array(Some(items)) = &reply else {
        panic!("HELLO must reply a map (flat array in RESP2), got {reply:?}");
    };
    assert_eq!(items.len() % 2, 0, "map must be key/value pairs: {items:?}");
    for field in [
        "server", "version", "proto", "id", "mode", "role", "modules",
    ] {
        assert!(map_get(&reply, field).is_some(), "missing field {field}");
    }
    assert_eq!(
        map_get(&reply, "proto"),
        Some(RespValue::int(2)),
        "RESP2 HELLO must report proto 2"
    );
    assert_eq!(
        map_get(&reply, "role"),
        Some(RespValue::bulk(bytes::Bytes::from_static(b"master")))
    );
    assert_eq!(
        map_get(&reply, "modules"),
        Some(RespValue::array(vec![])),
        "modules must be an empty array"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hello_then_ordinary_commands_still_work() {
    let dir = unique_dir("hello-then");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    let reply = resp(&mut s, &arr(&["HELLO"]));
    assert!(matches!(reply, RespValue::Array(Some(_))), "got {reply:?}");
    assert_eq!(resp(&mut s, &arr(&["SET", "hk", "hv"])), RespValue::ok());
    assert_eq!(
        resp(&mut s, &arr(&["GET", "hk"])),
        RespValue::bulk(bytes::Bytes::from_static(b"hv"))
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn raw(s: &mut TcpStream, payload: &[u8]) -> Vec<u8> {
    s.write_all(payload).unwrap();
    s.flush().unwrap();
    let mut buf = vec![0u8; 4096];
    let n = s.read(&mut buf).expect("read reply");
    buf[..n].to_vec()
}

#[test]
fn hello_accepts_protocol_3_with_resp3_map_reply() {
    let dir = unique_dir("hello-resp3");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    let reply = raw(&mut s, &arr(&["HELLO", "3"]));
    assert!(
        reply.starts_with(b"%7\r\n"),
        "HELLO 3 must reply a RESP3 map, got {:?}",
        String::from_utf8_lossy(&reply[..reply.len().min(40)])
    );
    assert!(
        reply.windows(15).any(|w| w == b"$5\r\nproto\r\n:3\r\n"),
        "proto must be integer 3, got {:?}",
        String::from_utf8_lossy(&reply)
    );
}

#[test]
fn hello_3_connection_still_serves_commands() {
    let dir = unique_dir("hello-resp3-cmd");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    let reply = raw(&mut s, &arr(&["HELLO", "3"]));
    assert!(reply.starts_with(b"%7\r\n"), "got {reply:?}");
    assert_eq!(resp(&mut s, &arr(&["SET", "r3", "v3"])), RespValue::ok());
    assert_eq!(
        resp(&mut s, &arr(&["GET", "r3"])),
        RespValue::bulk(bytes::Bytes::from_static(b"v3"))
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn resp2_connection_replies_null_bulk() {
    let dir = unique_dir("resp2-null");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    let reply = raw(&mut s, &arr(&["GET", "missing"]));
    assert_eq!(reply, b"$-1\r\n".to_vec(), "RESP2 keeps $-1 null bulk");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hello_3_replies_resp3_null() {
    let dir = unique_dir("resp3-null");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    let reply = raw(&mut s, &arr(&["HELLO", "3"]));
    assert!(reply.starts_with(b"%7\r\n"), "got {reply:?}");
    let reply = raw(&mut s, &arr(&["GET", "missing"]));
    assert_eq!(reply, b"_\r\n".to_vec(), "RESP3 null must be _");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hello_3_replies_hgetall_as_map() {
    let dir = unique_dir("resp3-hgetall");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    assert!(raw(&mut s, &arr(&["HELLO", "3"])).starts_with(b"%7\r\n"));
    assert_eq!(
        resp(&mut s, &arr(&["HSET", "h", "f1", "v1"])),
        RespValue::int(1)
    );
    let reply = raw(&mut s, &arr(&["HGETALL", "h"]));
    assert_eq!(
        reply,
        b"%1\r\n$2\r\nf1\r\n$2\r\nv1\r\n".to_vec(),
        "RESP3 HGETALL must be a map"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hello_3_replies_config_get_as_map() {
    let dir = unique_dir("resp3-config");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    assert!(raw(&mut s, &arr(&["HELLO", "3"])).starts_with(b"%7\r\n"));
    let reply = raw(&mut s, &arr(&["CONFIG", "GET", "maxmemory"]));
    assert!(
        reply.starts_with(b"%1\r\n"),
        "RESP3 CONFIG GET must be a map, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hello_3_groups_zrange_withscores_into_pairs() {
    let dir = unique_dir("resp3-zrange");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    assert!(raw(&mut s, &arr(&["HELLO", "3"])).starts_with(b"%7\r\n"));
    assert_eq!(
        resp(&mut s, &arr(&["ZADD", "z", "1", "one"])),
        RespValue::int(1)
    );
    let reply = raw(&mut s, &arr(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]));
    assert_eq!(
        reply,
        b"*1\r\n*2\r\n$3\r\none\r\n$1\r\n1\r\n".to_vec(),
        "RESP3 ZRANGE WITHSCORES must be array of pairs"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hello_rejects_unknown_protocol_version() {
    let dir = unique_dir("hello4");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    let reply = resp(&mut s, &arr(&["HELLO", "4"]));
    assert!(
        matches!(&reply, RespValue::Error(m) if m.contains("NOPROTO")),
        "unknown protocol versions must be refused with NOPROTO, got {reply:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hello_auth_authes_the_connection() {
    let pass = "hello-pass";
    let hash = hex::encode(jx_crypto::sm3_hash(pass.as_bytes()));
    let dir = unique_dir("hello-auth");
    let port = spawn_server(&dir, &format!("requirepass-sm3 {hash}\n"));
    let mut s = connect(port);
    let reply = resp(&mut s, &arr(&["HELLO", "2", "AUTH", "default", pass]));
    assert!(
        matches!(&reply, RespValue::Array(Some(_))),
        "HELLO with correct AUTH must succeed, got {reply:?}"
    );
    assert_eq!(
        resp(&mut s, &arr(&["GET", "x"])),
        RespValue::null_bulk(),
        "connection must be authenticated after HELLO AUTH"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hello_auth_with_wrong_password_does_not_auth() {
    let hash = hex::encode(jx_crypto::sm3_hash(b"real"));
    let dir = unique_dir("hello-badauth");
    let port = spawn_server(&dir, &format!("requirepass-sm3 {hash}\n"));
    let mut s = connect(port);
    let reply = resp(&mut s, &arr(&["HELLO", "2", "AUTH", "default", "wrong"]));
    assert!(
        matches!(&reply, RespValue::Error(m) if m.starts_with("WRONGPASS")),
        "bad HELLO AUTH must fail, got {reply:?}"
    );
    let reply = resp(&mut s, &arr(&["PING"]));
    assert!(
        matches!(&reply, RespValue::Error(m) if m.starts_with("NOAUTH")),
        "connection must stay unauthenticated, got {reply:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn scan_iterates_every_key_across_shards() {
    let dir = unique_dir("scan");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    for i in 0..25 {
        assert_eq!(
            resp(&mut s, &arr(&["SET", &format!("k{i}"), "v"])),
            RespValue::ok()
        );
    }

    let mut seen: Vec<String> = Vec::new();
    let mut cursor = "0".to_string();
    loop {
        let reply = resp(&mut s, &arr(&["SCAN", &cursor, "COUNT", "7"]));
        let RespValue::Array(Some(items)) = &reply else {
            panic!("SCAN must reply [cursor, keys], got {reply:?}");
        };
        assert_eq!(items.len(), 2, "got {items:?}");
        let RespValue::BulkString(Some(next)) = &items[0] else {
            panic!("cursor must be a bulk string, got {items:?}");
        };
        let RespValue::Array(Some(keys)) = &items[1] else {
            panic!("second element must be an array, got {items:?}");
        };
        for k in keys {
            let RespValue::BulkString(Some(k)) = k else {
                panic!("key must be bulk string");
            };
            seen.push(String::from_utf8_lossy(k).to_string());
        }
        let next = String::from_utf8_lossy(next).to_string();
        if next == "0" {
            break;
        }
        assert!(cursor != next, "cursor must advance, stuck at {next}");
        cursor = next;
    }

    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 25, "SCAN must return every key once: {seen:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn scan_match_and_count_options() {
    let dir = unique_dir("scan-match");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    for k in ["user:1", "user:2", "post:1"] {
        assert_eq!(resp(&mut s, &arr(&["SET", k, "v"])), RespValue::ok());
    }
    let reply = resp(
        &mut s,
        &arr(&["SCAN", "0", "MATCH", "user:*", "COUNT", "10"]),
    );
    let RespValue::Array(Some(items)) = &reply else {
        panic!("got {reply:?}");
    };
    let RespValue::Array(Some(keys)) = &items[1] else {
        panic!("got {items:?}");
    };
    let mut got: Vec<String> = keys
        .iter()
        .map(|k| match k {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
            other => panic!("{other:?}"),
        })
        .collect();
    got.sort();
    assert_eq!(got, vec!["user:1".to_string(), "user:2".to_string()]);
    assert_eq!(
        items[0],
        RespValue::bulk(bytes::Bytes::from_static(b"0")),
        "full scan ends with cursor 0"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn scan_rejects_negative_cursor() {
    let dir = unique_dir("scan-bad");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    let reply = resp(&mut s, &arr(&["SCAN", "-1"]));
    assert!(
        matches!(&reply, RespValue::Error(_)),
        "negative cursor must be rejected, got {reply:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn inline_lf_only_commands_are_answered_over_tcp() {
    let dir = unique_dir("inline-lf");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    assert_eq!(
        resp(&mut s, b"SET lfk lfv\n"),
        RespValue::ok(),
        "LF-only inline SET must be answered"
    );
    assert_eq!(
        resp(&mut s, b"GET lfk\n"),
        RespValue::bulk(bytes::Bytes::from_static(b"lfv")),
        "LF-only inline GET must be answered"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn inline_commands_are_answered_over_tcp() {
    let dir = unique_dir("inline");
    let port = spawn_server(&dir, "");
    let mut s = connect(port);
    assert_eq!(
        resp(&mut s, b"PING\r\n"),
        RespValue::SimpleString("PONG".into())
    );
    assert_eq!(resp(&mut s, b"SET inlinek inlinev\r\n"), RespValue::ok());
    assert_eq!(
        resp(&mut s, b"GET inlinek\r\n"),
        RespValue::bulk(bytes::Bytes::from_static(b"inlinev"))
    );
    let _ = std::fs::remove_dir_all(&dir);
}
