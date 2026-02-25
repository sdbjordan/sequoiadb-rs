use std::sync::atomic::{AtomicU64, Ordering};

/// Global metrics counters.
pub struct Metrics {
    pub total_insert: AtomicU64,
    pub total_query: AtomicU64,
    pub total_update: AtomicU64,
    pub total_delete: AtomicU64,
    pub total_read_bytes: AtomicU64,
    pub total_write_bytes: AtomicU64,
    pub active_sessions: AtomicU64,
    pub active_contexts: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            total_insert: AtomicU64::new(0),
            total_query: AtomicU64::new(0),
            total_update: AtomicU64::new(0),
            total_delete: AtomicU64::new(0),
            total_read_bytes: AtomicU64::new(0),
            total_write_bytes: AtomicU64::new(0),
            active_sessions: AtomicU64::new(0),
            active_contexts: AtomicU64::new(0),
        }
    }

    pub fn inc_insert(&self) {
        self.total_insert.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_query(&self) {
        self.total_query.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_update(&self) {
        self.total_update.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_delete(&self) {
        self.total_delete.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
