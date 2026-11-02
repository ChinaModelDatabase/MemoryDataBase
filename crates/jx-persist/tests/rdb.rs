use bytes::Bytes;
use jx_engine::actor::spawn_shard_runtime;
use jx_engine::system_now_ms;
use jx_persist::{load_rdb, save_rdb};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-rdb-{}-{tag}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn rt(n: usize) -> jx_engine::actor::ShardRuntime {
    spawn_shard_runtime(n, 1024, system_now_ms)
}

#[test]
fn rdb_save_load_roundtrip() {
    let e = rt(2);
    e.exec(vec![b("SET"), b("a"), b("1")]);
    e.exec(vec![b("SET"), b("b"), b("2")]);
    e.exec(vec![b("RPUSH"), b("l"), b("x"), b("y")]);
    let dir = tmp_dir("roundtrip");
    let path = dir.join("dump.rdb");
    save_rdb(&e, &path, None).unwrap();
    let e2 = rt(2);
    let n = load_rdb(&e2, &path, None).unwrap();
    assert_eq!(n, 3);
    assert_eq!(
        e2.exec(vec![b("GET"), b("a")]),
        jx_proto::RespValue::bulk(b("1"))
    );
    assert_eq!(
        e2.exec(vec![b("LLEN"), b("l")]),
        jx_proto::RespValue::int(2)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdb_encrypted_roundtrip() {
    let key = [1u8; 32];
    let e = rt(1);
    e.exec(vec![b("SET"), b("k"), b("secret")]);
    let dir = tmp_dir("enc");
    let path = dir.join("dump.rdb");
    save_rdb(&e, &path, Some(&key)).unwrap();
    let e2 = rt(1);
    load_rdb(&e2, &path, Some(&key)).unwrap();
    assert_eq!(
        e2.exec(vec![b("GET"), b("k")]),
        jx_proto::RespValue::bulk(b("secret"))
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// RDB must roundtrip TTL metadata through ShardRuntime save → load into a
/// fresh runtime: value readable and PTTL still within the original window.
#[test]
fn rdb_roundtrip_preserves_ttl() {
    let e = rt(2);
    e.exec(vec![b("SET"), b("ttl"), b("v"), b("PX"), b("60000")]);
    e.exec(vec![b("SET"), b("nottl"), b("w")]);
    let dir = tmp_dir("ttl");
    let path = dir.join("dump.rdb");
    save_rdb(&e, &path, None).unwrap();
    let e2 = rt(2);
    load_rdb(&e2, &path, None).unwrap();
    assert_eq!(
        e2.exec(vec![b("GET"), b("ttl")]),
        jx_proto::RespValue::bulk(b("v"))
    );
    let pttl = e2.exec(vec![b("PTTL"), b("ttl")]);
    assert!(
        matches!(pttl, jx_proto::RespValue::Integer(n) if n > 0 && n <= 60000),
        "loaded key must keep its expire timestamp, got {pttl:?}"
    );
    let ttl2 = e2.exec(vec![b("TTL"), b("nottl")]);
    assert_eq!(
        ttl2,
        jx_proto::RespValue::int(-1),
        "key without TTL stays persistent"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdb_encrypted_wrong_or_missing_key_fails() {
    let key = [1u8; 32];
    let e = rt(1);
    e.exec(vec![b("SET"), b("k"), b("v")]);
    let dir = tmp_dir("enc-err");
    let path = dir.join("dump.rdb");
    save_rdb(&e, &path, Some(&key)).unwrap();
    let e2 = rt(1);
    assert!(load_rdb(&e2, &path, None).is_err());
    let bad = [2u8; 32];
    assert!(load_rdb(&e2, &path, Some(&bad)).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdb_load_accounts_memory() {
    let e = rt(2);
    e.exec(vec![b("SET"), b("a"), b("1")]);
    e.exec(vec![b("SET"), b("b"), b("2")]);
    let dir = tmp_dir("load-mem");
    let path = dir.join("dump.rdb");
    save_rdb(&e, &path, None).unwrap();
    let e2 = rt(2);
    assert_eq!(e2.total_mem(), 0, "fresh runtime starts with zero mem");
    let n = load_rdb(&e2, &path, None).unwrap();
    assert_eq!(n, 2);
    assert!(
        e2.total_mem() > 0,
        "load_shard must account entry_mem into shard.mem"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdb_shard_count_mismatch_fails() {
    let e = rt(2);
    e.exec(vec![b("SET"), b("a"), b("1")]);
    let dir = tmp_dir("shard-mismatch");
    let path = dir.join("dump.rdb");
    save_rdb(&e, &path, None).unwrap();
    let e4 = rt(4);
    let err =
        load_rdb(&e4, &path, None).expect_err("2-shard rdb must not load into 4-shard runtime");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("shard count") && msg.contains('2') && msg.contains('4'),
        "error should mention both shard counts, got: {msg}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdb_corrupt_file_fails() {
    let dir = tmp_dir("corrupt");
    let path = dir.join("dump.rdb");
    std::fs::write(&path, b"not an rdb file at all........").unwrap();
    let e = rt(1);
    assert!(load_rdb(&e, &path, None).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Concurrent `save_rdb` calls must each succeed and leave a valid final
/// snapshot. Requires unique per-save tmp names; a fixed `dump.rdb.tmp` races
/// on rename and fails one writer.
#[test]
fn concurrent_save_rdb_all_succeed_with_unique_tmps() {
    let e = Arc::new(rt(2));
    e.exec(vec![b("SET"), b("a"), b("1")]);
    e.exec(vec![b("SET"), b("b"), b("2")]);
    let dir = tmp_dir("concurrent");
    let path = dir.join("dump.rdb");
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let e = e.clone();
            let path = path.clone();
            std::thread::spawn(move || save_rdb(&e, &path, None))
        })
        .collect();
    for h in handles {
        h.join()
            .unwrap()
            .expect("concurrent save_rdb must all succeed");
    }
    let e2 = rt(2);
    let n = load_rdb(&e2, &path, None).expect("final dump.rdb must be a valid snapshot");
    assert_eq!(n, 2);
    let leftovers: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|r| r.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|name| name.ends_with(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "tmp files must be renamed away, leftovers: {leftovers:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
