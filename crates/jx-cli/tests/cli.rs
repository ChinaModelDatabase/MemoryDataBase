use bytes::Bytes;
use jx_engine::actor::spawn_shard_runtime;
use jx_engine::system_now_ms;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(tag: &str) -> PathBuf {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let dir = std::env::temp_dir().join(format!("jx-cli-{}-{tag}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn keygen_creates_file() {
    let dir = tmp_dir("keygen");
    let out = dir.join("k.key");
    jx_cli::keygen::run(&out).unwrap();
    assert!(out.exists());
    assert_eq!(std::fs::read(&out).unwrap().len(), 32);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn check_config_ok() {
    let dir = tmp_dir("cfg-ok");
    let p = dir.join("MemoryDataBase.conf");
    std::fs::write(&p, "bind 127.0.0.1\nport 6399\n").unwrap();
    let cfg = jx_cli::check_config::run(&p).unwrap();
    assert_eq!(cfg.port, 6399);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn check_config_rejects_bad_port() {
    let dir = tmp_dir("cfg-bad");
    let p = dir.join("MemoryDataBase.conf");
    std::fs::write(&p, "bind 127.0.0.1\nport 0\n").unwrap();
    assert!(jx_cli::check_config::run(&p).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdb_info_plain_counts_keys() {
    let dir = tmp_dir("rdb-plain");
    let rdb = dir.join("dump.rdb");
    let e = spawn_shard_runtime(2, 1024, system_now_ms);
    e.exec(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"a"),
        Bytes::from_static(b"1"),
    ]);
    e.exec(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"b"),
        Bytes::from_static(b"2"),
    ]);
    jx_persist::save_rdb(&e, &rdb, None).unwrap();

    let info = jx_cli::rdb_info::run(&rdb, None).unwrap();
    assert!(!info.encrypted);
    assert_eq!(info.shard_count, Some(2));
    assert_eq!(info.keys, Some(2));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rdb_info_encrypted_requires_key() {
    let dir = tmp_dir("rdb-enc");
    let rdb = dir.join("dump.rdb");
    let key = [7u8; 32];
    let e = spawn_shard_runtime(1, 1024, system_now_ms);
    e.exec(vec![
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"k"),
        Bytes::from_static(b"v"),
    ]);
    jx_persist::save_rdb(&e, &rdb, Some(&key)).unwrap();

    let info = jx_cli::rdb_info::run(&rdb, None).unwrap();
    assert!(info.encrypted);
    assert_eq!(info.keys, None);

    let info = jx_cli::rdb_info::run(&rdb, Some(&key)).unwrap();
    assert!(info.encrypted);
    assert_eq!(info.shard_count, Some(1));
    assert_eq!(info.keys, Some(1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bench_percentile_math() {
    let lat = vec![10u64, 20, 30, 40, 50];
    let (p50, p99) = jx_cli::bench::percentiles(&lat);
    assert_eq!(p50, 30);
    assert_eq!(p99, 50);

    let lat = vec![1u64];
    let (p50, p99) = jx_cli::bench::percentiles(&lat);
    assert_eq!(p50, 1);
    assert_eq!(p99, 1);

    let mut lat: Vec<u64> = (1..=100).collect();
    lat.reverse();
    let (p50, p99) = jx_cli::bench::percentiles(&lat);
    assert_eq!(p50, 50);
    assert_eq!(p99, 99);
}

#[test]
fn bench_summarize_pipeline1_uses_request_latency_labels() {
    // 800ns and 1200ns samples → avg 1.0µs, p50/p99 in micros from nanos.
    let stats = jx_cli::bench::summarize(vec![800, 1200], 2, 0, 1_000_000, 1);
    assert_eq!(stats.ops, 2);
    assert_eq!(stats.errs, 0);
    assert!((stats.avg_us - 1.0).abs() < 1e-9);
    assert!((stats.p50_us - 0.8).abs() < 1e-9);
    assert!((stats.p99_us - 1.2).abs() < 1e-9);

    let line = jx_cli::bench::format_stats("SET", &stats);
    assert!(line.contains("ops=2"), "{line}");
    // 2 ops / 1ms phase wall = 2000 ops/s
    assert!(line.contains("ops/s=2000"), "{line}");
    assert!(line.contains("avg_us=1.0"), "{line}");
    assert!(line.contains("p50_us=0.8"), "{line}");
    assert!(line.contains("p99_us=1.2"), "{line}");
    assert!(line.contains("errs=0"), "{line}");
    assert!(!line.contains("batch"), "{line}");
}

#[test]
fn bench_summarize_pipeline_gt1_uses_batch_labels() {
    // batch wall samples in nanos; ops counts requests (not batches).
    let stats = jx_cli::bench::summarize(vec![10_000, 20_000], 8, 1, 2_000_000, 4);
    assert_eq!(stats.ops, 8);
    assert_eq!(stats.errs, 1);
    assert!((stats.avg_us - 15.0).abs() < 1e-9);
    assert!((stats.p50_us - 10.0).abs() < 1e-9);
    assert!((stats.p99_us - 20.0).abs() < 1e-9);

    let line = jx_cli::bench::format_stats("GET", &stats);
    assert!(line.contains("ops=8"), "{line}");
    // 8 ops / 2ms wall = 4000 ops/s
    assert!(line.contains("ops/s=4000"), "{line}");
    assert!(line.contains("batch_avg_us=15.0"), "{line}");
    assert!(line.contains("p50_batch_us=10.0"), "{line}");
    assert!(line.contains("p99_batch_us=20.0"), "{line}");
    assert!(line.contains("errs=1"), "{line}");
    assert!(!line.contains(" avg_us="), "{line}");
    assert!(!line.contains(" p50_us="), "{line}");
}

#[test]
fn bench_ops_per_s_uses_phase_wall_only() {
    // ops/s denominator is the per-phase wall (nanos), not total bench wall.
    let stats = jx_cli::bench::summarize(vec![500, 500], 100, 0, 10_000_000, 1);
    let line = jx_cli::bench::format_stats("SET", &stats);
    // 100 ops / 10ms = 10000 ops/s
    assert!(line.contains("ops/s=10000"), "{line}");
}

#[test]
fn cli_parses_subcommands() {
    use clap::Parser;
    let cli = jx_cli::Cli::try_parse_from(["jx-cli", "keygen", "--out", "k.key"]).unwrap();
    match cli.cmd {
        jx_cli::Cmd::Keygen { out } => assert_eq!(out, PathBuf::from("k.key")),
        _ => panic!("expected keygen"),
    }

    let cli = jx_cli::Cli::try_parse_from([
        "jx-cli",
        "bench",
        "--host",
        "127.0.0.1",
        "--port",
        "6379",
        "--clients",
        "4",
        "--requests",
        "100",
        "--value-size",
        "32",
        "--pipeline",
        "2",
    ])
    .unwrap();
    match cli.cmd {
        jx_cli::Cmd::Bench {
            clients,
            requests,
            pipeline,
            ..
        } => {
            assert_eq!(clients, 4);
            assert_eq!(requests, 100);
            assert_eq!(pipeline, 2);
        }
        _ => panic!("expected bench"),
    }

    let cli = jx_cli::Cli::try_parse_from(["jx-cli", "rdb-info", "--path", "dump.rdb"]).unwrap();
    match cli.cmd {
        jx_cli::Cmd::RdbInfo { path, key } => {
            assert_eq!(path, PathBuf::from("dump.rdb"));
            assert!(key.is_none());
        }
        _ => panic!("expected rdb-info"),
    }
}
