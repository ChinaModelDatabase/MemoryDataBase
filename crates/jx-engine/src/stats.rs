//! Process-wide monotonic counters (INFO stats / /metrics source).
//!
//! Relaxed ordering is sufficient: counters are observability-only and never
//! gate correctness.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct EngineStats {
    pub evicted_keys: AtomicU64,
    pub expired_keys: AtomicU64,
    pub rejected_connections: AtomicU64,
    pub conn_received: AtomicU64,
    pub cmd_processed: AtomicU64,
    pub cmd_errors: AtomicU64,
    pub queue_full: AtomicU64,
    pub shard_queue_depth_max: AtomicU64,
    pub aof_flushes: AtomicU64,
    pub aof_flush_latency_us_total: AtomicU64,
    pub aof_flush_latency_us_max: AtomicU64,
    pub rdb_saves: AtomicU64,
    pub rdb_saves_ms_last: AtomicU64,
}

pub static STATS: EngineStats = EngineStats {
    evicted_keys: AtomicU64::new(0),
    expired_keys: AtomicU64::new(0),
    rejected_connections: AtomicU64::new(0),
    conn_received: AtomicU64::new(0),
    cmd_processed: AtomicU64::new(0),
    cmd_errors: AtomicU64::new(0),
    queue_full: AtomicU64::new(0),
    shard_queue_depth_max: AtomicU64::new(0),
    aof_flushes: AtomicU64::new(0),
    aof_flush_latency_us_total: AtomicU64::new(0),
    aof_flush_latency_us_max: AtomicU64::new(0),
    rdb_saves: AtomicU64::new(0),
    rdb_saves_ms_last: AtomicU64::new(0),
};

#[inline]
pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn add(counter: &AtomicU64, n: u64) {
    if n > 0 {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

#[inline]
pub fn set_max(counter: &AtomicU64, v: u64) {
    let mut cur = counter.load(Ordering::Relaxed);
    while v > cur {
        match counter.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}

pub fn record_aof_flush(latency_us: u64) {
    inc(&STATS.aof_flushes);
    STATS
        .aof_flush_latency_us_total
        .fetch_add(latency_us, Ordering::Relaxed);
    set_max(&STATS.aof_flush_latency_us_max, latency_us);
}

pub fn record_rdb_save(duration_ms: u64) {
    inc(&STATS.rdb_saves);
    STATS
        .rdb_saves_ms_last
        .store(duration_ms, Ordering::Relaxed);
}

/// Render the `# Stats` INFO section (trailing \r\n included).
pub fn info_stats_section() -> String {
    let s = &STATS;
    let flushes = s.aof_flushes.load(Ordering::Relaxed);
    let flush_total = s.aof_flush_latency_us_total.load(Ordering::Relaxed);
    let flush_avg = flush_total.checked_div(flushes).unwrap_or(0);
    format!(
        "# Stats\r\n\
         total_connections_received:{}\r\n\
         rejected_connections:{}\r\n\
         total_commands_processed:{}\r\n\
         total_errors:{}\r\n\
         expired_keys:{}\r\n\
         evicted_keys:{}\r\n\
         keyspace_hits:0\r\n\
         keyspace_misses:0\r\n\
         queue_full:{}\r\n\
         shard_queue_depth_max:{}\r\n\
         aof_flushes:{}\r\n\
         aof_flush_latency_us_avg:{}\r\n\
         aof_flush_latency_us_max:{}\r\n\
         rdb_saves:{}\r\n\
         rdb_last_save_ms:{}\r\n",
        s.conn_received.load(Ordering::Relaxed),
        s.rejected_connections.load(Ordering::Relaxed),
        s.cmd_processed.load(Ordering::Relaxed),
        s.cmd_errors.load(Ordering::Relaxed),
        s.expired_keys.load(Ordering::Relaxed),
        s.evicted_keys.load(Ordering::Relaxed),
        s.queue_full.load(Ordering::Relaxed),
        s.shard_queue_depth_max.load(Ordering::Relaxed),
        flushes,
        flush_avg,
        s.aof_flush_latency_us_max.load(Ordering::Relaxed),
        s.rdb_saves.load(Ordering::Relaxed),
        s.rdb_saves_ms_last.load(Ordering::Relaxed),
    )
}
