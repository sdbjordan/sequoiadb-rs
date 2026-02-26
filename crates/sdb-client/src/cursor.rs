use sdb_bson::Document;

/// Client-side cursor for iterating query results.
///
/// V1: buffer-only, no GetMore support. All results are returned at once.
pub struct ClientCursor {
    buffer: Vec<Document>,
    pos: usize,
    closed: bool,
}

impl ClientCursor {
    /// Create a cursor from a vec of documents (returned by server reply).
    pub fn new(docs: Vec<Document>) -> Self {
        Self {
            buffer: docs,
            pos: 0,
            closed: false,
        }
    }

    /// Create an empty cursor.
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Advance to the next document. Returns `None` when exhausted.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Document> {
        if self.closed || self.pos >= self.buffer.len() {
            self.closed = true;
            return None;
        }
        let doc = self.buffer[self.pos].clone();
        self.pos += 1;
        Some(doc)
    }

    /// Mark the cursor as closed.
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Consume the cursor and collect all remaining documents.
    pub fn collect_all(self) -> Vec<Document> {
        if self.closed {
            return Vec::new();
        }
        self.buffer.into_iter().skip(self.pos).collect()
    }
}
