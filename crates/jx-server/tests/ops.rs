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
fn info_over_tcp_plain() {
    let dir = unique_dir("info");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, b"*1\r\n$4\r\nINFO\r\n");
    assert!(resp.contains("MemoryDataBase_version"), "got {resp:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn config_get_set_over_tcp() {
    let dir = unique_dir("config");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(
        &mut s,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$9\r\nmaxmemory\r\n$7\r\n2097152\r\n",
    );
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n",
    );
    assert!(resp.contains("2097152"), "got {resp:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn config_runtime_get_set_over_tcp() {
    let dir = unique_dir("config-rt");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");

    // Unset maxmemory GET returns "0" (Redis unlimited convention).
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n",
    );
    assert!(resp.contains("$1\r\n0\r\n"), "got {resp:?}");

    // appendonly reports the actual configured value (here: no).
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n",
    );
    assert!(
        resp.contains("appendonly") && resp.contains("no"),
        "got {resp:?}"
    );

    // loglevel GET/SET.
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$8\r\nloglevel\r\n",
    );
    assert!(resp.contains("info"), "got {resp:?}");
    let resp = cmd(
        &mut s,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$8\r\nloglevel\r\n$5\r\ndebug\r\n",
    );
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$8\r\nloglevel\r\n",
    );
    assert!(resp.contains("debug"), "got {resp:?}");

    // timeout GET/SET.
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$7\r\ntimeout\r\n",
    );
    assert!(resp.contains("$1\r\n0\r\n"), "got {resp:?}");
    let resp = cmd(
        &mut s,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$7\r\ntimeout\r\n$2\r\n30\r\n",
    );
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$7\r\ntimeout\r\n",
    );
    assert!(resp.contains("30"), "got {resp:?}");

    // requirepass-sm3 GET/SET — GET is redacted to (hidden) / empty.
    let newhash = hex::encode(jx_crypto::sm3_hash(b"newpass"));
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$15\r\nrequirepass-sm3\r\n",
    );
    assert!(resp.contains("requirepass-sm3"), "got {resp:?}");
    assert!(
        !resp.contains(&newhash),
        "unset requirepass must not leak, got {resp:?}"
    );
    let resp = cmd(
        &mut s,
        format!(
            "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$15\r\nrequirepass-sm3\r\n${}\r\n{}\r\n",
            newhash.len(),
            newhash
        )
        .as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$15\r\nrequirepass-sm3\r\n",
    );
    assert!(
        resp.contains("(hidden)"),
        "configured requirepass-sm3 must be redacted, got {resp:?}"
    );
    assert!(
        !resp.contains(&newhash),
        "GET must not leak the hash, got {resp:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn config_maxmemory_zero_and_policy_only_no_oom_over_tcp() {
    let dir = unique_dir("config-mm0");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");

    // Policy-only SET on an engine with maxmemory never set must not OOM later SETs.
    let resp = cmd(
        &mut s,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$16\r\nmaxmemory-policy\r\n$11\r\nallkeys-lru\r\n",
    );
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    let resp = cmd(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    assert_eq!(
        resp, "+OK\r\n",
        "policy-only must not OOM SET, got {resp:?}"
    );
    let resp = cmd(
        &mut s,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n",
    );
    assert!(
        resp.contains("$1\r\n0\r\n"),
        "unset maxmemory GET 0, got {resp:?}"
    );

    // CONFIG SET maxmemory 0 then SET works.
    let resp = cmd(
        &mut s,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$9\r\nmaxmemory\r\n$1\r\n0\r\n",
    );
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");
    let resp = cmd(&mut s, b"*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$1\r\nv\r\n");
    assert_eq!(
        resp, "+OK\r\n",
        "SET after maxmemory 0 must work, got {resp:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn config_requirepass_sm3_live_updates_auth() {
    let old_pass = "secret";
    let old_hash = hex::encode(jx_crypto::sm3_hash(old_pass.as_bytes()));
    let dir = unique_dir("config-auth");
    let port = spawn_server(&dir, &format!("requirepass-sm3 {old_hash}\n"));

    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(
        &mut s,
        format!(
            "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
            old_pass.len(),
            old_pass
        )
        .as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");

    let new_pass = "newpass";
    let new_hash = hex::encode(jx_crypto::sm3_hash(new_pass.as_bytes()));
    let resp = cmd(
        &mut s,
        format!(
            "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$15\r\nrequirepass-sm3\r\n${}\r\n{}\r\n",
            new_hash.len(),
            new_hash
        )
        .as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n", "got {resp:?}");

    // New connection must require auth with the NEW hash.
    let mut s2 = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s2, b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    assert!(resp.starts_with("-NOAUTH"), "got {resp:?}");
    let resp = cmd(
        &mut s2,
        format!(
            "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
            old_pass.len(),
            old_pass
        )
        .as_bytes(),
    );
    assert!(
        resp.starts_with("-WRONGPASS"),
        "old pass must fail after live update, got {resp:?}"
    );
    let resp = cmd(
        &mut s2,
        format!(
            "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
            new_pass.len(),
            new_pass
        )
        .as_bytes(),
    );
    assert_eq!(resp, "+OK\r\n", "new pass must work, got {resp:?}");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pipeline regression: SET/GET/INCR sent in one write must be executed
/// in order and the responses must arrive ordered in a single read.
#[test]
fn pipeline_responses_in_order() {
    let dir = unique_dir("pipeline");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.write_all(
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n\
          *2\r\n$3\r\nGET\r\n$1\r\na\r\n\
          *2\r\n$4\r\nINCR\r\n$1\r\nn\r\n",
    )
    .unwrap();
    s.flush().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut buf = vec![0u8; 256];
    let n = s.read(&mut buf).unwrap();
    let got = String::from_utf8_lossy(&buf[..n]).to_string();
    let ok = got.find("+OK\r\n").unwrap_or(usize::MAX);
    let get = got.find("$1\r\n1\r\n").unwrap_or(usize::MAX);
    let incr = got.find(":1\r\n").unwrap_or(usize::MAX);
    assert!(
        ok != usize::MAX && get != usize::MAX && incr != usize::MAX,
        "missing responses in single read: {got:?}"
    );
    assert!(ok < get && get < incr, "responses out of order: {got:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Pipeline batching: a multi-command read must be encoded into one buffer
/// and flushed with a single `write_all` (counted on a mock stream).
mod pipeline_batch {
    use std::io::Cursor;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use jx_engine::actor::spawn_shard_runtime;
    use jx_engine::system_now_ms;
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    struct CountingStream {
        reads: Cursor<Vec<u8>>,
        writes: Arc<Mutex<Vec<u8>>>,
        write_calls: Arc<AtomicUsize>,
    }

    impl AsyncRead for CountingStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let pos = self.reads.position() as usize;
            let data = self.reads.get_ref();
            if pos >= data.len() {
                return Poll::Ready(Ok(()));
            }
            let n = std::cmp::min(data.len() - pos, buf.remaining());
            buf.put_slice(&data[pos..pos + n]);
            self.reads.set_position((pos + n) as u64);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for CountingStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            this.write_calls.fetch_add(1, Ordering::SeqCst);
            this.writes.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn pipeline_responses_batched_into_single_write() {
        let rt = Arc::new(spawn_shard_runtime(2, 1024, system_now_ms));
        let runtime = Arc::new(std::sync::RwLock::new(jx_server::ConfigRuntime {
            loglevel: "info".into(),
            timeout_secs: 0,
            requirepass_sm3: None,
            appendonly: false,
            tls_mode: jx_server::config::TlsMode::Plain,
            role: jx_server::repl::Role::Standalone,
            replicaof: None,
            masterauth: None,
            repl_port: 0,
            repl_backlog_size: 0,
            bind: "127.0.0.1".into(),
            repl_timeout_secs: 60,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            slowlog: std::sync::Arc::new(jx_server::slowlog::Slowlog::new(
                jx_server::slowlog::SlowlogConfig {
                    log_slower_than: 10000,
                    max_len: 128,
                },
            )),
        }));
        let write_calls = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let input = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n\
                      *2\r\n$3\r\nGET\r\n$1\r\na\r\n\
                      *2\r\n$4\r\nINCR\r\n$1\r\nn\r\n"
            .to_vec();
        let stream = CountingStream {
            reads: Cursor::new(input),
            writes: writes.clone(),
            write_calls: write_calls.clone(),
        };
        jx_server::conn::handle_conn(
            stream,
            rt,
            runtime,
            None,
            jx_server::repl::ReplHandle::default(),
            1024,
        )
        .await
        .expect("handle_conn");
        assert_eq!(
            write_calls.load(Ordering::SeqCst),
            1,
            "pipeline must be encoded into a single write_all"
        );
        let got = String::from_utf8_lossy(&writes.lock().unwrap()).to_string();
        assert_eq!(got, "+OK\r\n$1\r\n1\r\n:1\r\n", "got {got:?}");
    }

    #[tokio::test]
    async fn pipeline_auth_and_quit_inside_batch() {
        let pass = "s3cret";
        let hash = hex::encode(jx_crypto::sm3_hash(pass.as_bytes()));
        let rt = Arc::new(spawn_shard_runtime(2, 1024, system_now_ms));
        let runtime = Arc::new(std::sync::RwLock::new(jx_server::ConfigRuntime {
            loglevel: "info".into(),
            timeout_secs: 0,
            requirepass_sm3: Some(hash),
            appendonly: false,
            tls_mode: jx_server::config::TlsMode::Plain,
            role: jx_server::repl::Role::Standalone,
            replicaof: None,
            masterauth: None,
            repl_port: 0,
            repl_backlog_size: 0,
            bind: "127.0.0.1".into(),
            repl_timeout_secs: 60,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            slowlog: std::sync::Arc::new(jx_server::slowlog::Slowlog::new(
                jx_server::slowlog::SlowlogConfig {
                    log_slower_than: 10000,
                    max_len: 128,
                },
            )),
        }));
        let write_calls = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(Mutex::new(Vec::new()));
        // AUTH / SET / QUIT in one pipeline. QUIT must end after the batch
        // is written (including the trailing +OK).
        let input = format!(
            "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n\
             *3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n\
             *1\r\n$4\r\nQUIT\r\n",
            pass.len(),
            pass
        )
        .into_bytes();
        let stream = CountingStream {
            reads: Cursor::new(input),
            writes: writes.clone(),
            write_calls: write_calls.clone(),
        };
        jx_server::conn::handle_conn(
            stream,
            rt,
            runtime,
            None,
            jx_server::repl::ReplHandle::default(),
            1024,
        )
        .await
        .expect("handle_conn");
        assert_eq!(
            write_calls.load(Ordering::SeqCst),
            1,
            "single batched write"
        );
        let got = String::from_utf8_lossy(&writes.lock().unwrap()).to_string();
        assert_eq!(got, "+OK\r\n+OK\r\n+OK\r\n", "got {got:?}");
    }
}

#[test]
fn keys_and_flushall_over_tcp() {
    let dir = unique_dir("keysflush");
    let port = spawn_server(&dir, "");
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let resp = cmd(&mut s, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = cmd(&mut s, b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n");
    assert!(resp.contains("foo"), "got {resp:?}");
    let resp = cmd(&mut s, b"*1\r\n$8\r\nFLUSHALL\r\n");
    assert_eq!(resp, "+OK\r\n");
    let resp = cmd(&mut s, b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n");
    assert!(!resp.contains("foo"), "got {resp:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Optional TLCP end-to-end check using jx-crypto fixtures.
/// Ignored by default: needs fixture PEMs and longer handshake setup.
#[test]
#[ignore = "tlcp handshake optional; run with --ignored"]
fn tlcp_mode_rejects_plain_client() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../jx-crypto/tests/fixtures/tlcp");
    if !fixtures.join("sign.crt").exists() {
        eprintln!("tlcp fixtures missing at {}; skipping", fixtures.display());
        return;
    }
    let dir = unique_dir("tlcp");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let conf = dir.join("jx.conf");
    std::fs::write(
        &conf,
        format!(
            "bind 127.0.0.1\nport {port}\ntls-mode tlcp\ndir {}\nappendonly no\nshards 1\ntlcp-sign-cert {}\ntlcp-sign-key {}\ntlcp-enc-cert {}\ntlcp-enc-key {}\ntlcp-ca {}\n",
            dir.display(),
            fixtures.join("sign.crt").display(),
            fixtures.join("sign.key").display(),
            fixtures.join("enc.crt").display(),
            fixtures.join("enc.key").display(),
            fixtures.join("ca.crt").display(),
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
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    // A plain RESP client cannot complete a TLCP handshake; the connection
    // must be closed without a valid PONG response.
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let _ = s.write_all(b"*1\r\n$4\r\nPING\r\n");
    let _ = s.flush();
    let mut buf = vec![0u8; 128];
    match s.read(&mut buf) {
        Ok(0) => {}
        Ok(n) => {
            let got = String::from_utf8_lossy(&buf[..n]);
            assert!(
                !got.contains("PONG"),
                "plain client got PONG over tlcp: {got:?}"
            );
        }
        Err(_) => {}
    }
    let _ = std::fs::remove_dir_all(&dir);
}
