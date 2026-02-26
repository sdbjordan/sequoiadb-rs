use sdb_bson::{Document, Value};
use sdb_common::Lsn;
use std::collections::HashMap;

const DEFAULT_BATCH_SIZE: usize = 100;

/// Server-side cursor holding remaining documents for a query.
struct ServerCursor {
    docs: Vec<Document>,
    pos: usize,
}

impl ServerCursor {
    fn new(docs: Vec<Document>) -> Self {
        Self { docs, pos: 0 }
    }

    fn next_batch(&mut self, count: usize) -> Vec<Document> {
        let end = (self.pos + count).min(self.docs.len());
        let batch: Vec<Document> = self.docs[self.pos..end].to_vec();
        self.pos = end;
        batch
    }

    fn is_exhausted(&self) -> bool {
        self.pos >= self.docs.len()
    }
}

/// Oplog tailing cursor — tracks a position in the WAL and returns new entries.
pub struct OplogCursor {
    /// WAL directory path for creating iterators.
    pub wal_path: String,
    /// LSN to scan from on next poll.
    pub resume_lsn: Lsn,
    /// Optional filter: only return ops matching this collection prefix.
    pub filter_collection: Option<String>,
}

/// Manages server-side cursors for batched query results.
pub struct CursorManager {
    cursors: HashMap<i64, ServerCursor>,
    oplog_cursors: HashMap<i64, OplogCursor>,
    next_id: i64,
    batch_size: usize,
}

impl CursorManager {
    pub fn new() -> Self {
        Self {
            cursors: HashMap::new(),
            oplog_cursors: HashMap::new(),
            next_id: 1,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Store docs in a cursor, return first batch and context_id.
    /// If all docs fit in one batch, returns context_id = -1 (no cursor needed).
    pub fn create_cursor(&mut self, docs: Vec<Document>) -> (Vec<Document>, i64) {
        if docs.len() <= self.batch_size {
            return (docs, -1);
        }
        let id = self.next_id;
        self.next_id += 1;
        let mut cursor = ServerCursor::new(docs);
        let first_batch = cursor.next_batch(self.batch_size);
        self.cursors.insert(id, cursor);
        (first_batch, id)
    }

    /// Create an oplog tailing cursor. Returns a context_id.
    pub fn create_oplog_cursor(
        &mut self,
        wal_path: String,
        start_lsn: Lsn,
        filter_collection: Option<String>,
    ) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        self.oplog_cursors.insert(
            id,
            OplogCursor {
                wal_path,
                resume_lsn: start_lsn,
                filter_collection,
            },
        );
        id
    }

    /// Check if a context_id belongs to an oplog cursor.
    pub fn is_oplog_cursor(&self, context_id: i64) -> bool {
        self.oplog_cursors.contains_key(&context_id)
    }

    /// Get mutable reference to an oplog cursor.
    pub fn get_oplog_cursor_mut(&mut self, context_id: i64) -> Option<&mut OplogCursor> {
        self.oplog_cursors.get_mut(&context_id)
    }

    /// Get more documents from an open cursor.
    /// Returns (batch, exhausted). Removes cursor when exhausted.
    pub fn get_more(&mut self, context_id: i64, num_to_return: i32) -> Option<(Vec<Document>, bool)> {
        let cursor = self.cursors.get_mut(&context_id)?;
        let count = if num_to_return <= 0 {
            self.batch_size
        } else {
            num_to_return as usize
        };
        let batch = cursor.next_batch(count);
        let exhausted = cursor.is_exhausted();
        if exhausted {
            self.cursors.remove(&context_id);
        }
        Some((batch, exhausted))
    }

    /// Kill a cursor, freeing its resources.
    pub fn kill(&mut self, context_id: i64) -> bool {
        self.cursors.remove(&context_id).is_some()
            || self.oplog_cursors.remove(&context_id).is_some()
    }
}

/// Convert a WAL LogRecord into a BSON Document for oplog output.
pub fn log_record_to_document(record: &sdb_dps::LogRecord) -> Document {
    let mut doc = Document::new();
    doc.insert("lsn", Value::Int64(record.lsn as i64));
    doc.insert("prevLsn", Value::Int64(record.prev_lsn as i64));
    doc.insert("txnId", Value::Int64(record.txn_id as i64));
    doc.insert("op", Value::String(record.op.as_str().to_string()));
    // Try to decode data as BSON document; fallback to raw length
    if let Ok(data_doc) = sdb_bson::Document::from_bytes(&record.data) {
        doc.insert("data", Value::Document(data_doc));
    } else if !record.data.is_empty() {
        doc.insert("dataLen", Value::Int32(record.data.len() as i32));
    }
    doc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_docs(n: usize) -> Vec<Document> {
        (0..n)
            .map(|i| {
                let mut d = Document::new();
                d.insert("i", Value::Int32(i as i32));
                d
            })
            .collect()
    }

    #[test]
    fn small_result_no_cursor() {
        let mut mgr = CursorManager::new();
        let (batch, ctx) = mgr.create_cursor(make_docs(10));
        assert_eq!(batch.len(), 10);
        assert_eq!(ctx, -1);
    }

    #[test]
    fn large_result_creates_cursor() {
        let mut mgr = CursorManager::new();
        let (batch, ctx) = mgr.create_cursor(make_docs(250));
        assert_eq!(batch.len(), 100);
        assert!(ctx > 0);

        // Get second batch
        let (batch2, exhausted) = mgr.get_more(ctx, -1).unwrap();
        assert_eq!(batch2.len(), 100);
        assert!(!exhausted);

        // Get third batch (remaining 50)
        let (batch3, exhausted) = mgr.get_more(ctx, -1).unwrap();
        assert_eq!(batch3.len(), 50);
        assert!(exhausted);

        // Cursor is gone
        assert!(mgr.get_more(ctx, -1).is_none());
    }

    #[test]
    fn kill_cursor() {
        let mut mgr = CursorManager::new();
        let (_, ctx) = mgr.create_cursor(make_docs(200));
        assert!(ctx > 0);
        assert!(mgr.kill(ctx));
        assert!(!mgr.kill(ctx)); // already gone
    }

    #[test]
    fn get_more_custom_batch_size() {
        let mut mgr = CursorManager::new();
        let (_, ctx) = mgr.create_cursor(make_docs(200));
        let (batch, _) = mgr.get_more(ctx, 50).unwrap();
        assert_eq!(batch.len(), 50);
    }

    #[test]
    fn oplog_cursor_create_and_kill() {
        let mut mgr = CursorManager::new();
        let ctx = mgr.create_oplog_cursor("/tmp/wal".into(), 32, None);
        assert!(ctx > 0);
        assert!(mgr.is_oplog_cursor(ctx));
        assert!(mgr.kill(ctx));
        assert!(!mgr.is_oplog_cursor(ctx));
    }

    #[test]
    fn oplog_cursor_with_filter() {
        let mut mgr = CursorManager::new();
        let ctx = mgr.create_oplog_cursor("/tmp/wal".into(), 32, Some("mycs.mycl".into()));
        let oc = mgr.get_oplog_cursor_mut(ctx).unwrap();
        assert_eq!(oc.filter_collection.as_deref(), Some("mycs.mycl"));
        assert_eq!(oc.resume_lsn, 32);
    }
}
