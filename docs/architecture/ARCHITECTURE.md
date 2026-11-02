# Architecture

## Threading
- Tokio I/O runtime: acceptor, TLS/TLCP, per-conn tasks, RESP parse/encode, dispatch, replication I/O.
- Per-shard OS actor threads: exclusive `ShardState`, bounded `crossbeam_channel`.
- Background: TTL (100ms), AOF fsync (everysec), RDB, metrics aggregation.

## Sharding
- `shard_id = hash(key) % shard_count` (fixed at startup). `shards=0` → auto.
- Cross-shard via `coordinator` (MGET coalesced/input-order, KEYS, FLUSHALL, DBSIZE, INFO, RANDOMKEY).

## Persistence
- RDB: forkless shard-sequential; `JX RDB\0\0` v1; temp+rename; shard-count validated on load; SM4-GCM optional.
- AOF: per-shard `appendonly.{sid}.aof` (plaintext RESP or `JXAOFENC`+SM4-GCM); combined rewrite; recovery: combined AOF base → skip RDB if present, else RDB+per-shard AOF. FLUSHALL/FLUSHDB shard-local during per-shard replay.

## Replication
- Master: repl listener (port+1), backlog, `on_write` tees on success; PSYNC/FULLRESYNC with RDB snapshot.
- Replica: `exec_no_aof` for master commands; READONLY for client writes; `REPLICAOF NO ONE` promotion.

## Backpressure & Fail-fast
- Bounded queues (capacity=maxclients). Full → `ERR shard command queue full` + ~1ms conn pause. Disconnected → fail-fast exit.

## Security
- SM3 auth (`requirepass-sm3` hex), SM4-GCM at-rest, TLCP (dual certs+CA) or TLS 1.3 terminated at conn layer.
