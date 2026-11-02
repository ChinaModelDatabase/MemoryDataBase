//! Single-flight SAVE/BGSAVE guard and shared last_save for shutdown_persist.
use bytes::Bytes;
use jx_engine::actor::spawn_shard_runtime;
use jx_engine::system_now_ms;
use jx_proto::RespValue;
use jx_server::persistence_cmd::{try_handle, PersistenceCtx};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-pctx-{}-{tag}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn ctx_in(dir: &std::path::Path) -> PersistenceCtx {
    let rt = Arc::new(spawn_shard_runtime(1, 1024, system_now_ms));
    PersistenceCtx::new(rt, dir.to_path_buf(), None)
}

/// While a BGSAVE worker holds the save lock, a second BGSAVE must return the
/// Redis-style single-flight error instead of starting another save.
#[test]
fn bgsave_rejected_while_save_in_progress() {
    let dir = unique_dir("bgsave-busy");
    let ctx = ctx_in(&dir);
    // Simulate in-flight save: lock held (try_lock fails) — also covered when
    // the atomic slot is claimed with the lock free.
    let _in_flight = ctx.save_lock.lock().unwrap();
    let resp = try_handle("BGSAVE", &ctx, &[]).expect("BGSAVE handled");
    assert_eq!(
        resp,
        RespValue::err("ERR Background save already in progress"),
        "second BGSAVE must fail single-flight"
    );
}

/// SAVE blocks on the same lock as BGSAVE; it must not complete while a save
/// is in flight.
#[test]
fn save_waits_for_in_flight_save_lock() {
    let dir = unique_dir("save-wait");
    let ctx = ctx_in(&dir);
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let lock = ctx.save_lock.clone();
    let holder = std::thread::spawn(move || {
        let _guard = lock.lock().unwrap();
        release_rx.recv().ok();
    });
    // Give the holder a moment to acquire the lock.
    std::thread::sleep(Duration::from_millis(50));

    let done = Arc::new(AtomicBool::new(false));
    let done2 = done.clone();
    let ctx2 = ctx.clone();
    let saver = std::thread::spawn(move || {
        let resp = try_handle("SAVE", &ctx2, &[]).expect("SAVE handled");
        done2.store(true, Ordering::SeqCst);
        resp
    });

    std::thread::sleep(Duration::from_millis(100));
    assert!(
        !done.load(Ordering::SeqCst),
        "SAVE must wait while the save lock is held"
    );
    release_tx.send(()).ok();
    let resp = saver.join().unwrap();
    assert_eq!(resp, RespValue::ok());
    assert!(
        ctx.last_save.load(Ordering::SeqCst) > 0,
        "SAVE must update last_save"
    );
    holder.join().unwrap();
}

/// `shutdown_persist` must update the same `last_save` AtomicI64 used by
/// SAVE/BGSAVE/LASTSAVE.
#[test]
fn shutdown_persist_updates_shared_last_save() {
    let dir = unique_dir("shut-lastsave");
    let rt = Arc::new(spawn_shard_runtime(1, 1024, system_now_ms));
    rt.exec(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"k"),
        Bytes::from_static(b"v"),
    ]);
    let cfg = jx_server::config::Config {
        bind: "127.0.0.1".into(),
        port: 0,
        tls_mode: jx_server::config::TlsMode::Plain,
        shards: Some(1),
        maxmemory: None,
        maxmemory_policy: "noeviction".into(),
        dir: dir.clone(),
        save: vec![],
        appendonly: false,
        appendfsync: "everysec".into(),
        requirepass_sm3: None,
        storage_key_file: None,
        tlcp: None,
        tls: None,
        maxclients: 1,
        timeout_secs: 0,
        loglevel: "info".into(),
        role: jx_server::config::Role::Standalone,
        replicaof: None,
        repl_port: 0,
        backlog_size: 64 * 1024 * 1024,
        repl_timeout_secs: 60,
        masterauth: None,
        slowlog_log_slower_than: 10000,
        slowlog_max_len: 128,
    };
    let pctx = PersistenceCtx::new(rt, dir.clone(), None);
    assert_eq!(
        pctx.last_save.load(Ordering::SeqCst),
        0,
        "precondition: last_save starts at 0"
    );
    jx_server::shutdown_persist(&pctx, &cfg).expect("shutdown_persist");
    assert!(
        pctx.last_save.load(Ordering::SeqCst) > 0,
        "shutdown_persist must update the shared last_save"
    );
    assert!(
        dir.join("dump.rdb").exists(),
        "shutdown must write dump.rdb"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
