# MemoryDataBase

MemoryDataBase (江湘) is a Rust-based, signal-processing-aware cache middleware targeting China's information innovation (信创) ecosystem. It provides RESP2 (Redis protocol) compatibility for seamless zero-modification migration of mainstream Redis clients, with a focus on single-node performance, persistence, national cryptography (SM2/SM3/SM4/TLCP), and cross-architecture deployment (x86_64, ARM64, LoongArch).

## Features

- **RESP2 Protocol Compatible**: Core command set covering Strings, Hashes, Lists, Sets, Sorted Sets, TTL, key management, and server administration.
- **High-Performance Sharded Architecture**: Tokio-based I/O with per-shard actor threads (exclusive ownership). Hot paths avoid global locks; cross-shard commands are coordinated and aggregated.
- **Persistence**: Forkless RDB snapshots (shard-sequential) with optional AOF (append-only log). Recovery policy: if `appendonly.aof` exists with `appendonly yes`, treat combined AOF as recovery base (skip RDB) and replay per-shard deltas; otherwise load RDB then replay per-shard AOF. Supports SM4-GCM file encryption.
- **Replication (HA Foundation)**: Master maintains circular backlog, supports PSYNC/FULLRESYNC, writes are tee'd to replicas on success (errors not propagated). Replica uses `exec_no_aof` for master commands to avoid double-appending AOF.
- **National Cryptography**: SM3 for password authentication (`requirepass-sm3`), SM4-GCM for at-rest RDB/AOF encryption, and TLCP (GB/T 38636-2020) for transport (`tls-mode tlcp`). TLS 1.3 also supported.
- **Memory Management & Eviction**: `maxmemory` with policies `noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-lfu`, `volatile-ttl`. Lazy expiration + periodic active cleanup (100ms budgeted).
- **Backpressure & Safety**: Bounded per-shard command queues; on saturation returns `ERR shard command queue full` with lightweight conn pause. Actor crash triggers fail-fast process exit; recovery failures refuse startup.
- **Observability**: Structured logging, INFO sections (server/memory/persistence/stats/replication/cpu/security), metrics counters (evictions, expirations, queue depth, AOF flush latency), SLOWLOG and CLIENT subcommands (minimal).
- **RESP3 handshake compatibility**: `HELLO` is supported (versions 2 and 3). Clients that negotiate protocol 3 get RESP3-shaped replies (map replies, `_` nulls, WITHSCORES pairs) for that connection; RESP2 clients keep the classic encodings.
- **Cross-Platform (信创)**: Targets x86_64, aarch64 (ARM64), loongarch64. Validated against mainstream domestic OS baselines (Kylin/UOS/openEuler/Anolis).

## Architecture Overview

```text
┌───────────────────────────────────────────────────────────┐
│  Tokio Runtime (I/O thread pool)                         │
│  ┌──────────┐  ┌──────────┐  ┌────────────────────────┐  │
│  │ Acceptor │→ │ Conn N   │→ │ RESP parse / dispatch   │  │
│  └──────────┘  └────┬─────┘  └──────────┬─────────────┘  │
│         TLS/TLCP terminated here        │                │
└────────────────────────────────────────┼────────────────┘
                    crossbeam-channel     │
     ┌────────────────┬───────────────────┼──────────────┐
     ▼                ▼                   ▼              ▼
┌─────────┐      ┌─────────┐        ┌─────────┐    ┌────────────┐
│Shard 0  │      │Shard 1  │   ...  │Shard N-1│    │Coordinator  │
│Actor Thd│      │Actor Thd│        │Actor Thd│    │(cross-shard)│
│exclusive│      │exclusive│        │exclusive│    │             │
└────┬────┘      └────┬────┘        └────┬────┘    └────┬────────┘
     │ AOF buffer   │                  │                │
     └──────────────┴──────────┬───────┴────────────────┘
                              ▼
                        Background: TTL / RDB / AOF fsync / metrics
```

For a detailed architecture description (threading, message flow, persistence, replication, security, error semantics), see [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md).

## Quick Start

### Prerequisites

- Rust stable (MSRV 1.85 as per workspace; 1.71+ minimum). Check: `rustc --version && cargo --version`
- OpenSSL/crypto tooling not required for pure-Rust TLCP (vendored `gm-tlcp`).
- On Linux, standard build tools (`gcc/clang`, `pkg-config`) for some transitive deps if FFI fallback enabled (optional).

### Build

```bash
# Build all crates (debug)
cargo build --workspace

# Release build
cargo build --release --workspace
```

Binaries: `target/release/jx-server`, `target/release/jx-cli`.

### Generate Storage Key (SM4-GCM At-Rest)

For encrypted RDB/AOF, generate a 32-byte key file (mode 0600):

```bash
jx-cli keygen --out /etc/MemoryDataBase/storage.key
chmod 600 /etc/MemoryDataBase/storage.key
```

### Configure

Copy and edit the example config:

```bash
mkdir -p /var/lib/MemoryDataBase /etc/MemoryDataBase
cp config/MemoryDataBase.conf.example /etc/MemoryDataBase/MemoryDataBase.conf
# Edit bind/port, tls-mode, shards, maxmemory, appendonly, requirepass-sm3, storage-key-file, etc.
```

Notes:
- `requirepass-sm3` is the lowercase hex of `SM3(password)` (unsalted in MVP). Generate via `python3 -c "import hashlib; print(hashlib.sha256(b'pass').hexdigest())"` is not SM3; use a SM3 tool or set interactively via deployment tooling. (MVP stores/compares the configured hex.)
- `tls-mode`: `plain`, `tls`, or `tlcp`. TLCP requires `tlcp-sign-cert/key`, `tlcp-enc-cert/key`, `tlcp-ca`.
- `shards = 0` uses `min(number of CPUs, default)`.

### Start Server

```bash
target/release/jx-server /etc/MemoryDataBase/MemoryDataBase.conf
```

Logs are JSON-formatted via `tracing-subscriber` (configurable `loglevel`).

### Verify

```bash
# PING
printf "*1\r\n\$4\r\nPING\r\n" | nc 127.0.0.1 6379

# Or use redis-cli (protocol compatible)
redis-cli -p 6379 PING
# => PONG
```

## Persistence

- **RDB**: `save <seconds> <changes>`, `BGSAVE`, `SAVE`, `LASTSAVE`. Forkless shard-sequential snapshots; format `JX RDB\0\0` (version 1).
- **AOF**: `appendonly yes/no`, `appendfsync always|everysec|no`, `BGREWRITEAOF`. Per-shard `appendonly.{sid}.aof` (plaintext RESP stream) or encrypted with header `JXAOFENC` + SM4-GCM body.
- **Recovery**: Combined AOF (`appendonly.aof`) takes precedence when `appendonly yes` and present (skips RDB to avoid double-application). Otherwise RDB base + per-shard AOF deltas. Recovery failures refuse startup. RDB shard-count must match runtime (`shards` fixed at startup).
- **Cross-shard migration note**: Changing `shards` between save/load requires a fresh SAVE or AOF-only recovery (see [docs/ops/shard-migration.md](docs/ops/shard-migration.md)).

## Replication

- Master: `role master`, `repl-port` (default `port+1`), `backlog-size`, writes tee'd to replicas via backlog; `INFO replication` reports `master_repl_port`, `connected_slaves`, `master_repl_offset`.
- Replica: `role replica`, `replicaof <host> <port>` (master client port; connects to `port+1`), `masterauth`. Client writes return `READONLY`. Promotion: `REPLICAOF NO ONE` promotes to master (lazily spawns master repl listener if needed).
- PSYNC: `PSYNC ? -1` for full resync → `FULLRESYNC <runid> <offset>` + RDB snapshot (supports encrypted RDB); subsequent commands streamed from backlog/incremental. Replica replays master commands with `exec_no_aof`.

## Security & National Cryptography

- **Auth**: `requirepass-sm3 <hex of SM3(password)>`. Connection must AUTH before other commands unless password unset.
- **At-rest**: `storage-key-file` enables SM4-GCM for RDB/AOF. Generate with `jx-cli keygen`.
- **Transport**: `tls-mode plain|tls|tlcp`.
  - `tlcp`: requires dual certs (signing/encryption) + CA (`tlcp-sign-cert`, `tlcp-sign-key`, `tlcp-enc-cert`, `tlcp-enc-key`, `tlcp-ca`). Uses `gm-tlcp` (vendored pure Rust) with cipher suites aligned to TLCP/GM standards.
- **Hardening**: AUTH required gating, READONLY on replicas for writes, structured errors without leaking internal paths (debug-only), file permissions expected for key/certs.

## Observability & Operations

- **INFO**: `INFO [server|memory|persistence|stats|replication|cpu|security]` aggregates across shards.
- **Metrics counters** (exposed via stats): `evicted_keys`, `expired_keys`, `rejected_connections`, `aof_flush_latency_ms`, `aof_flushes`, `rdb_saves_ms`, `rdb_saves`, `cmd_processed`, `cmd_errors`, `queue_full`, `shard_queue_depth_max` (per-shard sampled), `conn_active`.
- **SLOWLOG**: `SLOWLOG GET [n]`, `SLOWLOG LEN`, `SLOWLOG RESET`. Config: `slowlog-log-slower-than <microseconds>` (default 10000), `slowlog-max-len <entries>` (default 128). Execution time measured per command (exclusive of network I/O in actor path).
- **CLIENT**: `CLIENT LIST`, `CLIENT KILL <ip:port>` (minimal subset; additional options tracked for expansion).
- **Prometheus** (optional): `/metrics` endpoint planned; see [docs/metrics.md](docs/metrics.md) for metric names and labels.
- **Config validation**: `jx-cli check-config --config <path>`.

## Performance & Backpressure

- **Throughput target** (MVP acceptance): ≥ 500000 OPS for plaintext RESP small-value GET/SET on x86_64 reference hardware (shards = cores, pipeline varies). TLCP reported separately (see [docs/bench/tlcp-bench.md](docs/bench/tlcp-bench.md)).
- **Backpressure**: Per-shard bounded channel (capacity = `maxclients` or 1024). On `TrySendError::Full`, shard runtime returns `ERR shard command queue full`; connection layer sleeps ~1ms before continuing pipeline (I1). This bounds memory/queue growth under overload without indefinite blocking.
- **Cross-shard**: MGET fan-out parallelized per shard group with input-order remap. KEYS/RANDOMKEY broadcast with aggregation. DBSIZE/INFO aggregate stats.

## Testing & Compatibility

- **Unit/Integration**: `cargo test --workspace` (engine, proto, persist, server, cli).
- **Compatibility smoke**: `tests/compat/run_compat.sh` spins release server with 2 shards and runs Python (`redis-py`) and Go (`go-redis`) smoke checks.
- **Extended compatibility**: Additional cases tracked under `tests/compat/` and integration tests in crate `jx-server/tests/`.
- **Fail-fast correctness**: Actor crashes → process exit; recovery failures → refuse startup; corrupted files → reject.

Run tests:
```bash
export PATH="$(dirname "$(rustup which cargo)"):$PATH"
cargo test --workspace
```

## Build Matrix & CI

CI enforces: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --workspace`, `cargo deny check licenses`. Targets: x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu, loongarch64-unknown-linux-gnu.

See [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) for platform notes and cross-compilation guidance.

## Cross-Shard Migration

Changing `shards` between RDB saves and loads is not supported (RDB is shard-partitioned; load validates shard count). Migration procedure and alternatives (AOF-only recovery, full resync via replication, controlled rewrite): [docs/ops/shard-migration.md](docs/ops/shard-migration.md).

## TLCP Benchmark Baseline

TLCP handshake and record-layer overhead are reported separately from plaintext. Baseline template, methodology, environment capture, and reporting format: [docs/bench/tlcp-bench.md](docs/bench/tlcp-bench.md).

## Metrics Reference

Complete metric names, types, labels, descriptions, and example `INFO stats` additions: [docs/metrics.md](docs/metrics.md).

## Contributing

- Follow Rust style (`cargo fmt`). No new comments unless requested.
- TDD where applicable: add failing tests, implement, verify.
- Never commit secrets. All deps must be MIT/Apache-2.0/BSD/ISC/Unlicense (enforced via `cargo deny`).
- Actor boundary: engine stays free of network/persist I/O; persist stays free of conn/TLCP specifics.

## License

Apache-2.0. See [LICENSE](LICENSE) if present, or workspace `license = "Apache-2.0"`.
