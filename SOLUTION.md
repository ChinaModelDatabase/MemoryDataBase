# MemoryDataBase – Solution Overview

## 1) Project Overview

MemoryDataBase (江湘) is a Rust-based, single-node cache middleware targeting China's 信创 ecosystem. It provides RESP2 protocol compatibility for zero-modification migration of mainstream Redis clients, with support for RDB+AOF persistence, replication, TTL/eviction, national cryptography (SM3/SM4-GCM/TLCP), and cross-architecture deployment (x86_64, aarch64, loongarch64).

## 2) Current State

The workspace contains 6 crates (jx-proto, jx-engine, jx-persist, jx-crypto, jx-server, jx-cli). Implemented features:

- RESP2 codec (types/encode/decode) with pipeline support.
- Sharded actor architecture: per-shard OS thread owns `ShardState`, bounded crossbeam channels; hot paths lock-free.
- Cross-shard coordinator (MGET coalesced with input-order remap, KEYS glob, FLUSHALL/DBSIZE/INFO aggregation, RANDOMKEY).
- TTL: lazy + active 100ms budgeted cycle; expiry counters tracked.
- Eviction: maxmemory with policies (noeviction/allkeys-lru/volatile-lru/allkeys-lfu/volatile-lfu/volatile-ttl); sampling-based; OOM returns error.
- RDB: forkless shard-sequential snapshots (format JX RDB v1), SM4-GCM encryption; shard-count validation on load.
- AOF: per-shard append files, combined rewrite, recovery precedence (combined AOF base → skip RDB when present), per-shard replay with shard-local FLUSHALL/FLUSHDB.
- Replication: master backlog + PSYNC/FULLRESYNC (RDB snapshot), write tee on success, replica uses `exec_no_aof`, `REPLICAOF NO ONE` promotion.
- National crypto: SM3 auth, SM4-GCM at-rest, TLCP transport (gm-tlcp) + TLS 1.3.
- Persistence ops: SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF, shutdown persistence.
- Observability scaffolding: structured logging, INFO sections, stats counters added (evicted/expired/rejected/queue/aof/rdb/conn/cmd).
- SLOWLOG (conn-layer intercept: GET/LEN/RESET, `slowlog-log-slower-than`/`slowlog-max-len`, per-server ring buffer) and CLIENT (LIST/KILL minimal).
- Backpressure: queue full → `ERR shard command queue full` + ~1ms conn pause.

## 3) Architecture

Tokio I/O + N shard actors. Cross-shard via coordinator; AOF sink per actor (plaintext attached after recovery). Background tasks: TTL, AOF fsync, RDB. Replication split into master/replica modules with shared `ReplHandle`. See [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md).

## 4) Persistence & Recovery

- RDB: `snapshot_to_bytes` per-shard; `save_rdb` temp+rename; `load_rdb` validates magic/version/shard count.
- AOF: per-shard `appendonly.{sid}.aof` (plaintext RESP or `JXAOFENC`+SM4-GCM). `rewrite_aof` writes combined. `recover_on_startup`: if `appendonly && appendonly.aof` exists → replay combined base, then per-shard deltas; else RDB base + per-shard AOF if appendonly. FLUSHALL/FLUSHDB are shard-local during per-shard replay.

## 5) Replication

Master maintains circular backlog, tracks offsets/slaves; on write success `on_write` tees to replicas (errors excluded). Replica PSYNC → FULLRESYNC + RDB, then incremental stream replayed via `exec_no_aof`. Role switching via `REPLICAOF`.

## 6) Security

AUTH compares `requirepass-sm3` (hex of SM3(password), MVP). At-rest SM4-GCM via `storage-key-file`. Transport `plain|tls|tlcp` (dual certs + CA). TLCP terminated at conn layer.

## 7) Observability

Global atomic counters (`jx-engine::stats`) wired at all probe sites: accept/AUTH-fail → `conn_received`/`rejected_connections`; conn layer → `total_commands_processed`/`total_errors`; actor → `queue_full`/`shard_queue_depth_max`; TTL (lazy+active) → `expired_keys`; eviction → `evicted_keys`; AOF fsync → `aof_flushes` + latency (total/max/avg); RDB save → `rdb_saves`/`rdb_last_save_ms`. Exposed under INFO `# Stats`. SLOWLOG: per-server ring buffer, conn-layer intercept, `slowlog-log-slower-than` (µs, 0=off), `slowlog-max-len`. CLIENT LIST/KILL minimal. See [docs/metrics.md](docs/metrics.md).

## 8) Backpressure

Bounded per-shard queue (capacity=maxclients). `try_send` Full → RESP error; conn sleeps ~1ms before continuing pipeline (I1). Disconnected → `ERR shard actor crashed`.

## 9) Migration (shard count)

RDB is shard-partitioned; load enforces equal shard count. Migration paths: AOF-only recovery after clean shutdown+BGREWRITEAOF (preferred), full resync via replication, or logical export/import. See [docs/ops/shard-migration.md](docs/ops/shard-migration.md).

## 10) Performance Baselines

Plaintext small-value GET/SET target ≥ 500000 OPS (x86_64 reference). TLCP reported separately with overhead analysis. Template: [docs/bench/tlcp-bench.md](docs/bench/tlcp-bench.md).

## 11) CI & Quality

GitHub Actions: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `cargo deny check licenses`. `deny.toml` (cargo-deny 0.20 schema) allows MIT/Apache-2.0/BSD/ISC/Unlicense/Unicode-3.0. All four verified locally: 221 tests pass, clippy -D warnings clean, `licenses ok`.

## 12) Testing

221 workspace tests including `tests/slowlog_client.rs` (SLOWLOG LEN/GET/RESET, CLIENT LIST/KILL, INFO # Stats fields, unknown/unsupported/empty-command error semantics). `tests/compat/run_compat.sh` runs redis-py and go-redis smoke against release build. Extended compat cases tracked under `tests/compat/`.

## 13) Build Matrix

Targets: `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `loongarch64-unknown-linux-gnu`. Pure-Rust deps preferred.

## 14) Key Files Added

- README.md (features, quick start, persistence, replication, security, observability, backpressure, testing, CI, migration)
- docs/architecture/ARCHITECTURE.md
- docs/metrics.md
- docs/ops/shard-migration.md
- docs/bench/tlcp-bench.md
- .github/workflows/ci.yml
- deny.toml (updated/created)
