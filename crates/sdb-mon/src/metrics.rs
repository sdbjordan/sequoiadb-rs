use std::sync::atomic::{AtomicU64, Ordering};

use sdb_bson::{Document, Value};

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

    pub fn add_read_bytes(&self, n: u64) {
        self.total_read_bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_write_bytes(&self, n: u64) {
        self.total_write_bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_sessions(&self) {
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_sessions(&self) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_contexts(&self) {
        self.active_contexts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_contexts(&self) {
        self.active_contexts.fetch_sub(1, Ordering::Relaxed);
    }

    /// Export current metrics as a BSON document.
    pub fn to_document(&self) -> Document {
        let mut doc = Document::new();
        doc.insert("TotalInsert", Value::Int64(self.total_insert.load(Ordering::Relaxed) as i64));
        doc.insert("TotalQuery", Value::Int64(self.total_query.load(Ordering::Relaxed) as i64));
        doc.insert("TotalUpdate", Value::Int64(self.total_update.load(Ordering::Relaxed) as i64));
        doc.insert("TotalDelete", Value::Int64(self.total_delete.load(Ordering::Relaxed) as i64));
        doc.insert("TotalReadBytes", Value::Int64(self.total_read_bytes.load(Ordering::Relaxed) as i64));
        doc.insert("TotalWriteBytes", Value::Int64(self.total_write_bytes.load(Ordering::Relaxed) as i64));
        doc.insert("ActiveSessions", Value::Int64(self.active_sessions.load(Ordering::Relaxed) as i64));
        doc.insert("ActiveContexts", Value::Int64(self.active_contexts.load(Ordering::Relaxed) as i64));
        doc
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.total_insert.store(0, Ordering::Relaxed);
        self.total_query.store(0, Ordering::Relaxed);
        self.total_update.store(0, Ordering::Relaxed);
        self.total_delete.store(0, Ordering::Relaxed);
        self.total_read_bytes.store(0, Ordering::Relaxed);
        self.total_write_bytes.store(0, Ordering::Relaxed);
        self.active_sessions.store(0, Ordering::Relaxed);
        self.active_contexts.store(0, Ordering::Relaxed);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_increment() {
        let m = Metrics::new();
        m.inc_insert();
        m.inc_insert();
        m.inc_query();
        m.inc_update();
        m.inc_delete();
        assert_eq!(m.total_insert.load(Ordering::Relaxed), 2);
        assert_eq!(m.total_query.load(Ordering::Relaxed), 1);
        assert_eq!(m.total_update.load(Ordering::Relaxed), 1);
        assert_eq!(m.total_delete.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn bytes_counters() {
        let m = Metrics::new();
        m.add_read_bytes(100);
        m.add_write_bytes(200);
        m.add_read_bytes(50);
        assert_eq!(m.total_read_bytes.load(Ordering::Relaxed), 150);
        assert_eq!(m.total_write_bytes.load(Ordering::Relaxed), 200);
    }

    #[test]
    fn session_context_counters() {
        let m = Metrics::new();
        m.inc_sessions();
        m.inc_sessions();
        m.inc_contexts();
        m.dec_sessions();
        assert_eq!(m.active_sessions.load(Ordering::Relaxed), 1);
        assert_eq!(m.active_contexts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn to_document_exports_all() {
        let m = Metrics::new();
        m.inc_insert();
        m.inc_query();
        let doc = m.to_document();
        assert_eq!(doc.get("TotalInsert"), Some(&Value::Int64(1)));
        assert_eq!(doc.get("TotalQuery"), Some(&Value::Int64(1)));
        assert_eq!(doc.get("TotalUpdate"), Some(&Value::Int64(0)));
    }

    #[test]
    fn reset_clears_all() {
        let m = Metrics::new();
        m.inc_insert();
        m.add_read_bytes(100);
        m.reset();
        assert_eq!(m.total_insert.load(Ordering::Relaxed), 0);
        assert_eq!(m.total_read_bytes.load(Ordering::Relaxed), 0);
    }
}
