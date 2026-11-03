use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct SlowlogEntry {
    pub id: u64,
    pub timestamp_us: i64,
    pub duration_us: i64,
    pub args: Vec<Bytes>,
}

#[derive(Clone, Copy, Debug)]
pub struct SlowlogConfig {
    pub log_slower_than: i64,
    pub max_len: usize,
}

pub struct Slowlog {
    config: Mutex<SlowlogConfig>,
    entries: Mutex<VecDeque<SlowlogEntry>>,
    next_id: AtomicU64,
}

impl std::fmt::Debug for Slowlog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cfg = self.config.lock().unwrap_or_else(|p| p.into_inner());
        let len = self.entries.lock().unwrap_or_else(|p| p.into_inner()).len();
        f.debug_struct("Slowlog")
            .field("config", &(*cfg))
            .field("entries_len", &len)
            .finish()
    }
}

impl Slowlog {
    pub fn new(config: SlowlogConfig) -> Self {
        Self {
            config: Mutex::new(config),
            entries: Mutex::new(VecDeque::with_capacity(config.max_len.max(1))),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn set_config(&self, config: SlowlogConfig) {
        *self.config.lock().unwrap_or_else(|p| p.into_inner()) = config;
        self.trim();
    }

    fn trim(&self) {
        let max = self
            .config
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .max_len;
        let mut e = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        while e.len() > max {
            e.pop_front();
        }
    }

    pub fn record(&self, args: &[Bytes], duration_us: i64) {
        let (threshold, max) = {
            let c = self.config.lock().unwrap_or_else(|p| p.into_inner());
            (c.log_slower_than, c.max_len)
        };
        // Redis semantics: a negative threshold disables logging, zero logs
        // every command.
        if threshold < 0 || duration_us < threshold {
            return;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| (d.as_secs() as i64) * 1_000_000 + (d.subsec_micros() as i64))
            .unwrap_or(0);
        let mut e = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        e.push_back(SlowlogEntry {
            id,
            timestamp_us: ts,
            duration_us,
            args: args.to_vec(),
        });
        while e.len() > max {
            e.pop_front();
        }
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn reset(&self) {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }

    /// Newest-first (Redis semantics), limited by `count`.
    pub fn get(&self, count: Option<usize>) -> Vec<SlowlogEntry> {
        let e = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let take = count.unwrap_or(e.len()).min(e.len());
        e.iter().rev().take(take).cloned().collect()
    }
}

static SLOWLOG: OnceLock<Arc<Slowlog>> = OnceLock::new();

/// Process-wide slowlog handle. First call creates it with `config`;
/// subsequent calls refresh the config from the caller's runtime view.
pub fn slowlog_handle(config: SlowlogConfig) -> Arc<Slowlog> {
    let sl = SLOWLOG.get_or_init(|| Arc::new(Slowlog::new(config)));
    sl.set_config(config);
    sl.clone()
}
