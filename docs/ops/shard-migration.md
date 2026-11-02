# Cross-Shard Migration Guide

## Background

MemoryDataBase stores data in shard-partitioned structures. RDB files are serialized per shard and include the `shard_count` in the header. At startup, `load_rdb` validates that the RDB's shard count equals the runtime's `shard_count` (from `shards` config). If they differ, loading fails with an error like:

```
rdb shard count X != runtime Y
```

This is a hard constraint by design (shard ownership is baked into RDB layout). Changing `shards` between a save and a subsequent load is **not supported** without migration.

## When Migration Is Needed

- Increasing/decreasing CPU count and wanting to rebalance across more/fewer shards.
- Changing deployment topology where `shards` was tuned differently.
- Moving data from a cluster of different shard sizing (rare for single-node).

## Migration Options

### Option 1: AOF-only recovery (recommended if AOF is complete)

If `appendonly yes` and the combined AOF (`appendonly.aof`) is present and consistent (from a clean shutdown or `BGREWRITEAOF`), recovery treats combined AOF as base and skips RDB. The AOF is a command stream (logical), not shard-partitioned by the old count in the same way. Therefore:

1. Stop old instance cleanly (SIGTERM) so `appendonly.aof` is rewritten or consistent.
2. Update `shards` in config to new value (e.g. N').
3. Start new instance with same `dir`. With `appendonly yes`, `recover_on_startup` will replay `appendonly.aof` as base, then per-shard deltas (if any). The command stream rebuilds state into N' shards.
4. Verify data via `DBSIZE`, `KEYS` sample, or client checks.

**Caveats**: Encrypted AOF/RDB paths still apply (same `storage-key-file`).

### Option 2: Full resync via replication (HA path)

If the node is/was a replica or you can stand up a new master:
1. Set up new instance as master with desired `shards = N'`.
2. Configure old instance as replica of new master (`REPLICAOF new_host new_port`) or use `PSYNC` flow. The new master will send a FULLRESYNC with RDB snapshot generated under N', and old replica will receive/apply it.
3. Once caught up, promote as needed (`REPLICAOF NO ONE`) and redirect clients.

This avoids manual RDB transformation and works well when both nodes are available.

### Option 3: Export/import via clients (logical dump)

For small datasets or cross-environment moves:
1. On source, iterate keys (e.g. `SCAN`/`KEYS` with care) and `DUMP`+`TTL` or `GET` type-aware reads, then `RESTORE`/`SET`+`EXPIRE` on target with new `shards`.
2. Target can have different `shards` (no RDB compatibility constraint). 
3. This is logical, safe, but slower and may have consistency window.

## Pre-Migration Checklist

- [ ] Confirm RDB shard count vs target: `jx-cli rdb-info --path dump.rdb` (shows `shards: X`).
- [ ] Verify AOF consistency: `appendonly.aof` exists and was written by clean shutdown or `BGREWRITEAOF`.
- [ ] Back up `dir/` (`dump.rdb`, `appendonly*.aof`, `storage.key`, config).
- [ ] Ensure target `storage-key-file` matches if encrypted.
- [ ] Plan client cutover (quiesce writes if strong consistency required).

## Operational Notes

- `shards` is fixed at startup; no online resharding in MVP.
- Actor crash = fail-fast; do not attempt to "hot change" shards.
- RDB shard-count check is enforced to prevent silent corruption/misloading.
- Per-shard AOF files (`appendonly.{sid}.aof`) are tied to runtime shard count at replay time (each shard replays its own file). After changing shards, old per-shard files are effectively deltas for a different partitioning and should not be mixed blindly except via Option 1 (combined AOF base path) or after a fresh save/rewrite under new count.
- Prefer Option 1 after clean shutdown with `appendonly yes` + `BGREWRITEAOF` (ensures combined AOF is a full-state snapshot in command form).

## Example Workflow (AOF-only)

```bash
# Source: clean shutdown
kill -TERM $(cat /var/run/jx-server.pid)
sleep 2

# Backup
cp -r /var/lib/MemoryDataBase /var/lib/MemoryDataBase.bak

# Update config: shards 8 (was 4)
sed -i 's/^shards 4/shards 8/' /etc/MemoryDataBase/MemoryDataBase.conf

# Start with new shards; recovery uses appendonly.aof as base
target/release/jx-server /etc/MemoryDataBase/MemoryDataBase.conf
```

If combined AOF is missing/corrupt, do not proceed with this method—use Option 2 or 3.
