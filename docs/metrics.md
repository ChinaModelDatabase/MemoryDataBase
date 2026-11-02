# Metrics Reference

This document defines metrics tracked by MemoryDataBase for observability, capacity planning, and performance analysis. Metrics are exposed via:
- `INFO stats` section (key counters)
- Prometheus `/metrics` endpoint (optional; implementation planned)

## Metric Naming

Prometheus-style names use `MemoryDataBase_` prefix. Types: counter (monotonic), gauge (point-in-time), histogram (latency).

## Core Engine Metrics

| Name | Type | Labels | Description |
|---|---|---|---|
| `MemoryDataBase_keys_total` | gauge | `shard` | Number of live keys in shard (aggregated in INFO `keys=`) |
| `MemoryDataBase_expires_total` | gauge | `shard` | Keys with TTL set (aggregated in INFO) |
| `MemoryDataBase_mem_bytes` | gauge | `shard` | Approximate memory used by shard (aggregated `used_memory`) |
| `MemoryDataBase_evicted_keys_total` | counter | - | Total keys evicted due to `maxmemory` (INFO `evicted_keys`) |
| `MemoryDataBase_expired_keys_total` | counter | - | Total keys expired/removed by TTL (lazy + active) (INFO `expired_keys`) |

## Command & Error Metrics

| Name | Type | Labels | Description |
|---|---|---|---|
| `MemoryDataBase_cmd_processed_total` | counter | `cmd` | Commands successfully processed (by command name) |
| `MemoryDataBase_cmd_errors_total` | counter | `cmd`, `errtype` | Commands returning RESP errors |
| `MemoryDataBase_rejected_connections_total` | counter | `reason` | Connections rejected (maxclients, auth fail, tls/tlcp handshake fail, etc.) (INFO `rejected_connections`) |

## Backpressure & Queue Metrics

| Name | Type | Labels | Description |
|---|---|---|---|
| `MemoryDataBase_queue_full_total` | counter | `shard` | Times shard command queue was full (`try_send` Full) |
| `MemoryDataBase_shard_queue_depth_max` | gauge | `shard` | Max observed queue depth in sampling window (capacity = `maxclients`) |
| `MemoryDataBase_conn_active` | gauge | - | Active client connections (INFO `connected_clients`) |

## Persistence Metrics

| Name | Type | Labels | Description |
|---|---|---|---|
| `MemoryDataBase_aof_flush_latency_ms` | histogram | - | Time to `fsync` AOF files (everysec/always). Buckets in ms. (INFO `aof_flush_latency_ms_avg`/p50/p99 planned) |
| `MemoryDataBase_aof_flushes_total` | counter | `policy` | Number of AOF fsync operations |
| `MemoryDataBase_rdb_saves_ms` | histogram | - | Duration of RDB save (BGSAVE/SAVE/shutdown) |
| `MemoryDataBase_rdb_saves_total` | counter | `trigger` | RDB saves by trigger (`bgsave`, `save`, `shutdown`) |
| `MemoryDataBase_aof_rewrites_total` | counter | `status` | AOF rewrites completed/failed |

## Replication Metrics

| Name | Type | Labels | Description |
|---|---|---|---|
| `MemoryDataBase_repl_connected_slaves` | gauge | - | Number of connected replicas (INFO `connected_slaves`) |
| `MemoryDataBase_repl_backlog_size_bytes` | gauge | - | Backlog size configured/used |
| `MemoryDataBase_repl_master_offset` | gauge | - | Master replication offset (INFO `master_repl_offset`) |
| `MemoryDataBase_repl_fullresyncs_total` | counter | - | Full resyncs performed |
| `MemoryDataBase_repl_sync_errors_total` | counter | `errtype` | Replication sync errors |

## TLCP/TLS Metrics

| Name | Type | Labels | Description |
|---|---|---|---|
| `MemoryDataBase_tlcp_handshakes_total` | counter | `status` | TLCP handshakes by success/fail |
| `MemoryDataBase_tls_handshakes_total` | counter | `status` | TLS handshakes by success/fail |
| `MemoryDataBase_tlcp_record_latency_ms` | histogram | - | TLCP record encrypt/decrypt latency (optional, depends on `gm-tlcp` metrics) |

## INFO stats Additions (implemented)

`INFO` now appends a `# Stats` section rendered by `jx_engine::stats::info_stats_section()`
(global `AtomicU64` counters, `Relaxed`). Fields (Redis-compatible naming where applicable):

- `total_connections_received`: TCP accepts since startup (`conn_received`)
- `rejected_connections`: AUTH failures + TLS/TLCP handshake failures
- `total_commands_processed`: non-error responses (`cmd_processed`)
- `total_errors`: RESP error responses (`cmd_errors`)
- `expired_keys`: lazy + active TTL removals
- `evicted_keys`: `maxmemory` evictions
- `keyspace_hits` / `keyspace_misses`: placeholder `0` (engine-side; not yet wired)
- `queue_full`: `ERR shard command queue full` occurrences (I1 backpressure)
- `shard_queue_depth_max`: max observed shard queue depth (sampled per recv)
- `aof_flushes`: AOF `sync_all` operations (everysec flusher + always per-append)
- `aof_flush_latency_us_avg` / `aof_flush_latency_us_max`: fsync latency (µs)
- `rdb_saves`: RDB snapshots completed (SAVE/BGSAVE/shutdown)
- `rdb_last_save_ms`: duration of the most recent RDB save (ms)

Probe sites: `net.rs` (accept/handshake), `conn.rs` (command outcome, AUTH),
`actor.rs` (queue full/depth), `ttl.rs` + `shard.rs::touch_alive` (expiry),
`evict.rs` (eviction), `jx-persist/aof.rs` (fsync), `jx-persist/rdb.rs` (save).

## Implementation Notes

- Counters use `std::sync::atomic::AtomicU64` (or `AtomicI64`) with `Ordering::Relaxed` for hot paths; snapshots read with `SeqCst` or `Relaxed` consistent with aggregation.
- Histograms: track count + sum + buckets (simple fixed buckets in ms: 1,5,10,25,50,100,250,500,1000,2500,5000,10000). 
- Per-shard gauges updated in actor thread; cross-shard aggregation via `broadcast`/atomic snapshots (as in `stats()`).
- `/metrics` format: text-based Prometheus exposition (Content-Type `text/plain; version=0.0.4`). Optional; disabled by default unless `metrics-enabled yes`.
