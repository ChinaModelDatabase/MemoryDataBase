use bytes::Bytes;
use jx_engine::actor::{spawn_shard_runtime, AofSink};
use jx_engine::system_now_ms;
use jx_persist::{
    append_aof, encode_command, replay_aof, replay_aof_shards, rewrite_aof, FileAofSink,
};
use jx_proto::RespValue;
use std::sync::Arc;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn rt(n: usize) -> jx_engine::actor::ShardRuntime {
    spawn_shard_runtime(n, 1024, system_now_ms)
}

#[test]
fn aof_append_replay() {
    let dir = std::env::temp_dir().join(format!("jx-aof-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    append_aof(&path, &[b("SET"), b("k"), b("v")], None).unwrap();
    append_aof(&path, &[b("RPUSH"), b("l"), b("a")], None).unwrap();
    let e = rt(1);
    let n = replay_aof(&e, &path, None).unwrap();
    assert_eq!(n, 2);
    assert_eq!(e.exec(vec![b("GET"), b("k")]), RespValue::bulk(b("v")));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aof_rewrite_full_snapshot_commands() {
    let e = rt(1);
    e.exec(vec![b("SET"), b("a"), b("1")]);
    e.exec(vec![b("SET"), b("b"), b("2")]);
    let dir = std::env::temp_dir().join(format!("jx-aof-rw-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    rewrite_aof(&e, &path, None).unwrap();
    let e2 = rt(1);
    replay_aof(&e2, &path, None).unwrap();
    assert_eq!(e2.exec(vec![b("GET"), b("a")]), RespValue::bulk(b("1")));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aof_encode_command_resp_array() {
    let payload = jx_persist::encode_command(&[b("SET"), b("k"), b("v")]);
    let mut dec = jx_proto::RespDecoder::new();
    dec.feed(&payload);
    let Ok(Some(RespValue::Array(Some(items)))) = dec.next_value() else {
        panic!("encode_command must produce a RESP array, got {payload:?}");
    };
    let args: Vec<Bytes> = items
        .into_iter()
        .filter_map(|x| match x {
            RespValue::BulkString(Some(bb)) => Some(bb),
            _ => None,
        })
        .collect();
    assert_eq!(args, vec![b("SET"), b("k"), b("v")]);
}

#[test]
fn aof_rewrite_collection_types() {
    let e = rt(1);
    e.exec(vec![b("RPUSH"), b("l"), b("x"), b("y")]);
    e.exec(vec![b("HSET"), b("h"), b("f"), b("v")]);
    e.exec(vec![b("SADD"), b("s"), b("m1")]);
    e.exec(vec![b("ZADD"), b("z"), b("1.5"), b("m")]);
    let dir = std::env::temp_dir().join(format!("jx-aof-coll-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    rewrite_aof(&e, &path, None).unwrap();
    let e2 = rt(1);
    let n = replay_aof(&e2, &path, None).unwrap();
    assert_eq!(n, 4);
    assert_eq!(e2.exec(vec![b("LLEN"), b("l")]), RespValue::int(2));
    assert_eq!(
        e2.exec(vec![b("HGET"), b("h"), b("f")]),
        RespValue::bulk(b("v"))
    );
    assert_eq!(e2.exec(vec![b("SCARD"), b("s")]), RespValue::int(1));
    assert_eq!(
        e2.exec(vec![b("ZSCORE"), b("z"), b("m")]),
        RespValue::bulk(b("1.5"))
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aof_encrypted_append_replay() {
    let key = [1u8; 32];
    let dir = std::env::temp_dir().join(format!("jx-aof-enc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    append_aof(&path, &[b("SET"), b("secret"), b("shh")], Some(&key)).unwrap();
    let e = rt(1);
    let n = replay_aof(&e, &path, Some(&key)).unwrap();
    assert_eq!(n, 1);
    assert_eq!(
        e.exec(vec![b("GET"), b("secret")]),
        RespValue::bulk(b("shh"))
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aof_encrypted_missing_or_wrong_key_fails() {
    let key = [1u8; 32];
    let dir = std::env::temp_dir().join(format!("jx-aof-encerr-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    append_aof(&path, &[b("SET"), b("k"), b("v")], Some(&key)).unwrap();
    let e = rt(1);
    assert!(replay_aof(&e, &path, None).is_err());
    let bad = [2u8; 32];
    assert!(replay_aof(&e, &path, Some(&bad)).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aof_rewrite_preserves_ttls_on_replay() {
    let e = rt(1);
    e.exec(vec![b("SET"), b("ttlkey"), b("v"), b("PX"), b("60000")]);
    e.exec(vec![b("SET"), b("nottl"), b("w")]);
    let dir = std::env::temp_dir().join(format!("jx-aof-ttl-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    rewrite_aof(&e, &path, None).unwrap();
    let e2 = rt(1);
    replay_aof(&e2, &path, None).unwrap();
    assert_eq!(
        e2.exec(vec![b("GET"), b("ttlkey")]),
        RespValue::bulk(b("v"))
    );
    let ttl = e2.exec(vec![b("PTTL"), b("ttlkey")]);
    assert!(
        matches!(ttl, RespValue::Integer(n) if n > 0 && n <= 60000),
        "rewrite must preserve TTL via PEXPIREAT, got {ttl:?}"
    );
    let ttl2 = e2.exec(vec![b("TTL"), b("nottl")]);
    assert_eq!(
        ttl2,
        RespValue::int(-1),
        "key without TTL must stay persistent"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aof_encrypted_rewrite_roundtrips_all_keys() {
    let key = [9u8; 32];
    let e = rt(2);
    e.exec(vec![b("SET"), b("a"), b("1"), b("EX"), b("100")]);
    e.exec(vec![b("SET"), b("b"), b("2")]);
    e.exec(vec![b("RPUSH"), b("l"), b("x"), b("y")]);
    e.exec(vec![b("HSET"), b("h"), b("f"), b("v")]);
    let dir = std::env::temp_dir().join(format!("jx-aof-enc-rw-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    rewrite_aof(&e, &path, Some(&key)).unwrap();
    // Rewritten encrypted AOF is a single JXAOFENC payload containing ALL commands.
    let raw = std::fs::read(&path).unwrap();
    assert!(
        raw.starts_with(b"JXAOFENC"),
        "encrypted rewrite must be a single sealed payload"
    );
    let e2 = rt(2);
    let n = replay_aof(&e2, &path, Some(&key)).unwrap();
    assert_eq!(n, 5, "all rewrite commands (incl. PEXPIREAT) must replay");
    assert_eq!(e2.exec(vec![b("GET"), b("a")]), RespValue::bulk(b("1")));
    assert_eq!(e2.exec(vec![b("GET"), b("b")]), RespValue::bulk(b("2")));
    assert_eq!(e2.exec(vec![b("LLEN"), b("l")]), RespValue::int(2));
    assert_eq!(
        e2.exec(vec![b("HGET"), b("h"), b("f")]),
        RespValue::bulk(b("v"))
    );
    let ttl = e2.exec(vec![b("PTTL"), b("a")]);
    assert!(
        matches!(ttl, RespValue::Integer(n) if n > 0 && n <= 100000),
        "encrypted rewrite must preserve TTL, got {ttl:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-aof-shard-{}-{tag}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn read_shard_files(dir: &std::path::Path, nshards: usize) -> Vec<(usize, Vec<u8>)> {
    (0..nshards)
        .filter_map(|sid| {
            let p = dir.join(format!("appendonly.{sid}.aof"));
            std::fs::read(&p).ok().map(|b| (sid, b))
        })
        .collect()
}

/// Actor-owned AOF: successful writes append RESP to `appendonly.{sid}.aof`;
/// reads and failed writes must not. Replay of the shard files restores state.
#[test]
fn aof_per_shard_append_replay_roundtrip() {
    let dir = tmp_dir("roundtrip");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    assert_eq!(e.exec(vec![b("SET"), b("a"), b("1")]), RespValue::ok());
    assert_eq!(e.exec(vec![b("SET"), b("b"), b("2")]), RespValue::ok());
    // Read must not append; wrong-arity write must not append.
    assert!(matches!(
        e.exec(vec![b("GET"), b("a")]),
        RespValue::BulkString(Some(_))
    ));
    assert!(matches!(e.exec(vec![b("SET")]), RespValue::Error(_)));

    let files = read_shard_files(&dir, 2);
    assert!(
        !files.is_empty(),
        "successful writes must create per-shard appendonly.{{sid}}.aof files"
    );
    let blob: Vec<u8> = files.iter().flat_map(|(_, b)| b.clone()).collect();
    let mut dec = jx_proto::RespDecoder::new();
    dec.feed(&blob);
    let mut cmds = 0u32;
    let mut saw_get = false;
    while let Ok(Some(v)) = dec.next_value() {
        cmds += 1;
        if let RespValue::Array(Some(items)) = v {
            if let Some(RespValue::BulkString(Some(name))) = items.first() {
                if name.as_ref() == b"GET" {
                    saw_get = true;
                }
            }
        }
    }
    assert_eq!(cmds, 2, "only the two successful SETs must be appended");
    assert!(!saw_get, "GET must not be appended to AOF");

    // Recovery path: fresh runtime replays the per-shard files.
    let e2 = spawn_shard_runtime(2, 1024, system_now_ms);
    let n = replay_aof_shards(&e2, &dir, None).unwrap();
    assert_eq!(n, 2);
    assert_eq!(e2.exec(vec![b("GET"), b("a")]), RespValue::bulk(b("1")));
    assert_eq!(e2.exec(vec![b("GET"), b("b")]), RespValue::bulk(b("2")));
    let _ = std::fs::remove_dir_all(&dir);
}

/// `exec_no_aof` must replay commands without touching shard AOF files
/// (used by recovery so replay does not re-append what it just loaded).
#[test]
fn aof_exec_no_aof_does_not_append() {
    let dir = tmp_dir("no-aof");
    let e = spawn_shard_runtime(1, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    e.exec_no_aof(vec![b("SET"), b("k"), b("v")]);
    assert!(
        read_shard_files(&dir, 1).is_empty(),
        "exec_no_aof must not append to shard AOF files"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Encrypted mode: live per-shard append is not wired (MVP single-command
/// seal limitation); durability comes from encrypted RDB + rewrite_aof.
#[test]
fn aof_encrypted_mode_skips_live_shard_append() {
    let dir = tmp_dir("enc-skip");
    let key = [3u8; 32];
    let e = spawn_shard_runtime(1, 1024, system_now_ms);
    // Server only attaches the sink for plaintext; simulate that by attaching
    // no sink — the encrypted durability path is rewrite/RDB (covered above).
    e.exec(vec![b("SET"), b("k"), b("v")]);
    assert!(
        read_shard_files(&dir, 1).is_empty(),
        "without a sink no shard files appear"
    );
    // rewrite_aof with key still produces a single sealed full-stream payload.
    rewrite_aof(&e, &dir.join("appendonly.aof"), Some(&key)).unwrap();
    let raw = std::fs::read(dir.join("appendonly.aof")).unwrap();
    assert!(raw.starts_with(b"JXAOFENC"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// rewrite_aof must clear per-shard append files after consolidating state
/// into the combined AOF, so the next recovery cannot double-replay.
#[test]
fn aof_rewrite_clears_shard_append_files() {
    let dir = tmp_dir("rewrite-clear");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    e.exec(vec![b("SET"), b("a"), b("1")]);
    e.exec(vec![b("SET"), b("b"), b("2")]);
    assert!(
        !read_shard_files(&dir, 2).is_empty(),
        "precondition: shard files exist"
    );
    rewrite_aof(&e, &dir.join("appendonly.aof"), None).unwrap();
    assert!(
        read_shard_files(&dir, 2).is_empty(),
        "rewrite must clear per-shard append files"
    );
    // Combined AOF still recovers full state.
    let e2 = spawn_shard_runtime(2, 1024, system_now_ms);
    replay_aof(&e2, &dir.join("appendonly.aof"), None).unwrap();
    assert_eq!(e2.exec(vec![b("GET"), b("a")]), RespValue::bulk(b("1")));
    assert_eq!(e2.exec(vec![b("GET"), b("b")]), RespValue::bulk(b("2")));
    let _ = std::fs::remove_dir_all(&dir);
}

/// encode_command is the RESP payload shape the per-shard sink appends.
#[test]
fn aof_file_aof_sink_appends_encoded_command() {
    let dir = tmp_dir("sink-shape");
    let sink = FileAofSink::new(dir.clone());
    sink.append(1, &[b("SET"), b("x"), b("y")]);
    let path = dir.join("appendonly.1.aof");
    let raw = std::fs::read(&path).unwrap();
    assert_eq!(raw, encode_command(&[b("SET"), b("x"), b("y")]));
    // Encoding matches what replay_aof expects.
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    replay_aof(&e, &path, None).unwrap();
    assert_eq!(e.exec(vec![b("GET"), b("x")]), RespValue::bulk(b("y")));
    let _ = std::fs::remove_dir_all(&dir);
}

/// DEL is logged per-shard; recovery must not resurrect deleted keys.
#[test]
fn aof_del_recovery_does_not_resurrect_key() {
    let dir = tmp_dir("del-recover");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    assert_eq!(e.exec(vec![b("SET"), b("keep"), b("1")]), RespValue::ok());
    assert_eq!(e.exec(vec![b("SET"), b("gone"), b("2")]), RespValue::ok());
    assert_eq!(e.exec(vec![b("DEL"), b("gone")]), RespValue::int(1));

    let e2 = spawn_shard_runtime(2, 1024, system_now_ms);
    let n = replay_aof_shards(&e2, &dir, None).unwrap();
    assert!(n >= 3, "replay must apply SET+DEL records, n={n}");
    assert_eq!(
        e2.exec(vec![b("GET"), b("gone")]),
        RespValue::null_bulk(),
        "deleted key must not resurrect after AOF recovery"
    );
    assert_eq!(e2.exec(vec![b("GET"), b("keep")]), RespValue::bulk(b("1")));
    let _ = std::fs::remove_dir_all(&dir);
}

/// FLUSHALL is logged on every shard; recovery must end with empty state.
#[test]
fn aof_flushall_recovery_clears_all_keys() {
    let dir = tmp_dir("flush-recover");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    e.exec(vec![b("SET"), b("a"), b("1")]);
    e.exec(vec![b("SET"), b("b"), b("2")]);
    assert_eq!(e.exec(vec![b("FLUSHALL")]), RespValue::ok());
    // Post-flush write must survive recovery (appended after FLUSHALL).
    e.exec(vec![b("SET"), b("after"), b("3")]);

    let e2 = spawn_shard_runtime(2, 1024, system_now_ms);
    replay_aof_shards(&e2, &dir, None).unwrap();
    assert_eq!(e2.exec(vec![b("GET"), b("a")]), RespValue::null_bulk());
    assert_eq!(e2.exec(vec![b("GET"), b("b")]), RespValue::null_bulk());
    assert_eq!(e2.exec(vec![b("GET"), b("after")]), RespValue::bulk(b("3")));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Cross-shard RENAME recovery: src must not resurrect; dst keeps the value.
#[test]
fn aof_cross_shard_rename_recovery() {
    let dir = tmp_dir("rename-recover");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    let mut src = None;
    let mut dst = None;
    for i in 0..64u32 {
        let k = Bytes::from(format!("rk{i}"));
        let sid = e.shard_id_for_key(&k);
        if src.is_none() {
            src = Some((k, sid));
        } else if Some(sid) != src.as_ref().map(|(_, s)| *s) {
            dst = Some(k);
            break;
        }
    }
    let (src, _) = src.expect("need src");
    let dst = dst.expect("need dst");
    assert_eq!(e.exec(vec![b("SET"), src.clone(), b("v")]), RespValue::ok());
    assert_eq!(
        e.exec(vec![b("RENAME"), src.clone(), dst.clone()]),
        RespValue::ok()
    );

    let e2 = spawn_shard_runtime(2, 1024, system_now_ms);
    replay_aof_shards(&e2, &dir, None).unwrap();
    assert_eq!(
        e2.exec(vec![b("EXISTS"), src.clone()]),
        RespValue::int(0),
        "src must not resurrect"
    );
    assert_eq!(
        e2.exec(vec![b("GET"), dst.clone()]),
        RespValue::bulk(b("v"))
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Cross-shard RENAME with a pre-existing dst container: recovery must replay
/// `DEL dst` on the destination shard **before** the value command, so dst
/// holds only the renamed list elements — not old+new.
#[test]
fn aof_cross_shard_rename_recovery_replaces_preexisting_dst() {
    let dir = tmp_dir("rename-dst-recover");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    let mut src = None;
    let mut dst = None;
    for i in 0..64u32 {
        let k = Bytes::from(format!("rk{i}"));
        let sid = e.shard_id_for_key(&k);
        if src.is_none() {
            src = Some((k, sid));
        } else if Some(sid) != src.as_ref().map(|(_, s)| *s) {
            dst = Some(k);
            break;
        }
    }
    let (src, _) = src.expect("need src");
    let dst = dst.expect("need dst");
    // Pre-existing dst container on the destination shard + src list value.
    assert_eq!(
        e.exec(vec![b("RPUSH"), dst.clone(), b("old")]),
        RespValue::Integer(1)
    );
    assert_eq!(
        e.exec(vec![b("RPUSH"), src.clone(), b("new")]),
        RespValue::Integer(1)
    );
    assert_eq!(
        e.exec(vec![b("RENAME"), src.clone(), dst.clone()]),
        RespValue::ok()
    );

    let e2 = spawn_shard_runtime(2, 1024, system_now_ms);
    replay_aof_shards(&e2, &dir, None).unwrap();
    assert_eq!(
        e2.exec(vec![b("EXISTS"), src.clone()]),
        RespValue::int(0),
        "src must not resurrect"
    );
    let got = e2.exec(vec![
        b("LRANGE"),
        dst.clone(),
        Bytes::from_static(b"0"),
        Bytes::from_static(b"-1"),
    ]);
    match got {
        RespValue::Array(Some(items)) => {
            assert_eq!(
                items.len(),
                1,
                "dst must contain only the renamed value, not old+new: {items:?}"
            );
            assert_eq!(
                items[0],
                RespValue::bulk(b("new")),
                "dst elements: {items:?}"
            );
        }
        other => panic!("LRANGE dst after recovery: {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// After SAVE (RDB + shard/combined AOF compact), crash-sim recovery loads
/// RDB only — INCR must not double-apply on top of the snapshot.
#[test]
fn save_compacts_aof_no_double_incr() {
    let dir = tmp_dir("save-compact");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    assert_eq!(e.exec(vec![b("SET"), b("k"), b("1")]), RespValue::ok());
    assert_eq!(e.exec(vec![b("INCR"), b("k")]), RespValue::int(2));
    assert_eq!(e.exec(vec![b("INCR"), b("k")]), RespValue::int(3));
    // Precondition: live AOF recorded the ops.
    assert!(
        !read_shard_files(&dir, 2).is_empty(),
        "shard AOF must exist before SAVE"
    );

    // SAVE path: RDB write + compact (same as server SAVE/BGSAVE).
    let rdb = dir.join("dump.rdb");
    jx_persist::save_rdb_compact_aof(&e, &rdb, None, &dir).expect("save_rdb_compact_aof");
    assert!(
        read_shard_files(&dir, 2).is_empty(),
        "SAVE must clear per-shard AOF files"
    );
    assert!(
        !dir.join("appendonly.aof").exists(),
        "SAVE must clear combined AOF so recovery is RDB-only"
    );

    // Crash-sim restart: fresh runtime, load RDB, do not replay AOF.
    let e2 = spawn_shard_runtime(2, 1024, system_now_ms);
    jx_persist::load_rdb(&e2, &rdb, None).expect("load_rdb");
    assert_eq!(
        e2.exec(vec![b("GET"), b("k")]),
        RespValue::bulk(b("3")),
        "value must not be double-incremented after SAVE recovery"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Without compact, leftover shard AOF replayed on top of RDB double-applies
/// INCR — documents why SAVE must clear shard files.
#[test]
fn save_without_compact_would_double_apply_incr() {
    let dir = tmp_dir("save-nocompact");
    let e = spawn_shard_runtime(1, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    e.exec(vec![b("INCR"), b("k")]); // → 1
    e.exec(vec![b("INCR"), b("k")]); // → 2
    let rdb = dir.join("dump.rdb");
    jx_persist::save_rdb(&e, &rdb, None).unwrap();
    // Simulate a SAVE that forgot to compact: RDB has 2, shard AOF still has INCRs.
    assert!(!read_shard_files(&dir, 1).is_empty());
    let e2 = spawn_shard_runtime(1, 1024, system_now_ms);
    jx_persist::load_rdb(&e2, &rdb, None).unwrap();
    replay_aof_shards(&e2, &dir, None).unwrap();
    assert_eq!(
        e2.exec(vec![b("GET"), b("k")]),
        RespValue::bulk(b("4")),
        "precondition: replaying uncleared AOF double-applies INCR (2→4)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// appendfsync policy parse + FileAofSink::with_fsync must produce a sink
/// that appends successfully under always/no/everysec.
#[test]
fn aof_fsync_policy_parse_and_sink_appends() {
    use jx_persist::FsyncPolicy;
    assert_eq!(FsyncPolicy::parse("always"), FsyncPolicy::Always);
    assert_eq!(FsyncPolicy::parse("everysec"), FsyncPolicy::EverySec);
    assert_eq!(FsyncPolicy::parse("no"), FsyncPolicy::No);
    assert_eq!(FsyncPolicy::parse("ALWAYS"), FsyncPolicy::Always);

    for policy in [FsyncPolicy::Always, FsyncPolicy::No, FsyncPolicy::EverySec] {
        let dir = tmp_dir(&format!("fsync-{policy:?}"));
        let sink = FileAofSink::with_fsync(dir.clone(), policy);
        sink.append(0, &[b("SET"), b("f"), b("1")]);
        let path = dir.join("appendonly.0.aof");
        let raw = std::fs::read(&path).expect("append must write shard file");
        assert_eq!(raw, encode_command(&[b("SET"), b("f"), b("1")]));
        drop(sink); // stops everysec flusher thread
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// C1: shutdown with appendonly writes both a full-state combined AOF
/// (rewrite_aof) and an RDB snapshot of the same moment. Recovery must treat
/// the combined AOF as the base (skip RDB) so RPUSH is not double-applied.
#[test]
fn aof_shutdown_rewrite_plus_rdb_recovery_does_not_double() {
    let dir = tmp_dir("shutdown-rpush");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    assert_eq!(
        e.exec(vec![b("RPUSH"), b("l"), b("1"), b("2"), b("3")]),
        RespValue::int(3)
    );

    // Shutdown(appendonly=true) path: rewrite_aof then save_rdb — both exist.
    let aof = dir.join("appendonly.aof");
    let rdb = dir.join("dump.rdb");
    rewrite_aof(&e, &aof, None).unwrap();
    jx_persist::save_rdb(&e, &rdb, None).unwrap();
    assert!(
        aof.exists() && rdb.exists(),
        "precondition: both files exist"
    );

    // Startup recovery with appendonly=true.
    let e2 = spawn_shard_runtime(2, 1024, system_now_ms);
    jx_persist::recover_on_startup(&e2, &dir, None, true).expect("recover_on_startup");
    let llen = e2.exec(vec![b("LLEN"), b("l")]);
    assert_eq!(
        llen,
        RespValue::int(3),
        "combined AOF is full-state base; RPUSH must not double on top of RDB, got {llen:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// C2: each per-shard AOF file logs FLUSHALL. Replay in sid order must apply
/// FLUSHALL shard-locally — a higher-sid file's FLUSHALL must not wipe
/// post-flush writes recorded in a lower-sid file replayed earlier.
#[test]
fn aof_per_shard_flushall_replay_is_shard_local() {
    let dir = tmp_dir("flush-local");
    let nshards = 4;
    let e = spawn_shard_runtime(nshards, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));

    // Find keys that land on sid0 and sid3.
    let mut key0 = None;
    let mut key3 = None;
    for i in 0..1024u32 {
        let k = Bytes::from(format!("fk{i}"));
        let sid = e.shard_id_for_key(&k);
        if sid == 0 && key0.is_none() {
            key0 = Some(k.clone());
        }
        if sid == 3 && key3.is_none() {
            key3 = Some(k);
        }
        if key0.is_some() && key3.is_some() {
            break;
        }
    }
    let key0 = key0.expect("need a key on sid0");
    let key3 = key3.expect("need a key on sid3");

    // Pre-flush writes (any shard), then FLUSHALL (logged to every shard file),
    // then post-flush writes on sid0 and sid3.
    assert_eq!(
        e.exec(vec![b("SET"), key0.clone(), b("pre")]),
        RespValue::ok()
    );
    assert_eq!(
        e.exec(vec![b("SET"), key3.clone(), b("pre")]),
        RespValue::ok()
    );
    assert_eq!(e.exec(vec![b("FLUSHALL")]), RespValue::ok());
    assert_eq!(
        e.exec(vec![b("SET"), key0.clone(), b("post0")]),
        RespValue::ok()
    );
    assert_eq!(
        e.exec(vec![b("SET"), key3.clone(), b("post3")]),
        RespValue::ok()
    );

    // Replay order is sid 0..nshards. Without shard-local FLUSHALL, sid1/2/3
    // files' FLUSHALL records (broadcast clear) wipe sid0's post-flush write.
    let e2 = spawn_shard_runtime(nshards, 1024, system_now_ms);
    replay_aof_shards(&e2, &dir, None).unwrap();
    assert_eq!(
        e2.exec(vec![b("GET"), key0.clone()]),
        RespValue::bulk(b("post0")),
        "sid0 post-flush write must survive higher-sid FLUSHALL replay"
    );
    assert_eq!(
        e2.exec(vec![b("GET"), key3.clone()]),
        RespValue::bulk(b("post3")),
        "sid3 post-flush write must survive recovery"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// C1 (no AOF): when combined AOF is absent (e.g. after SAVE compact), RDB is
/// the base and per-shard files are deltas on top.
#[test]
fn aof_recovery_rdb_base_when_combined_absent() {
    let dir = tmp_dir("rdb-base");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.set_aof_sink(Some(Arc::new(FileAofSink::new(dir.clone()))));
    assert_eq!(
        e.exec(vec![b("SET"), b("base"), b("from_rdb")]),
        RespValue::ok()
    );
    let rdb = dir.join("dump.rdb");
    // SAVE path: RDB + compact AOF away.
    jx_persist::save_rdb_compact_aof(&e, &rdb, None, &dir).unwrap();
    // Post-SAVE write lands only in per-shard files.
    assert_eq!(
        e.exec(vec![b("SET"), b("delta"), b("post_save")]),
        RespValue::ok()
    );
    assert!(
        !dir.join("appendonly.aof").exists(),
        "compact clears combined aof"
    );

    let e2 = spawn_shard_runtime(2, 1024, system_now_ms);
    jx_persist::recover_on_startup(&e2, &dir, None, true).expect("recover_on_startup");
    assert_eq!(
        e2.exec(vec![b("GET"), b("base")]),
        RespValue::bulk(b("from_rdb"))
    );
    assert_eq!(
        e2.exec(vec![b("GET"), b("delta")]),
        RespValue::bulk(b("post_save"))
    );
    let _ = std::fs::remove_dir_all(&dir);
}
