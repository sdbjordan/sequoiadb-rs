use sdb_bson::Document;
use sdb_common::Result;

/// Client-facing cursor for iterating query results.
pub struct Cursor {
    pub context_id: i64,
    buffer: Vec<Document>,
    pos: usize,
    exhausted: bool,
}

impl Cursor {
    pub fn new(context_id: i64) -> Self {
        Self {
            context_id,
            buffer: Vec::new(),
            pos: 0,
            exhausted: false,
        }
    }

    pub fn next(&mut self) -> Result<Option<Document>> {
        if self.pos < self.buffer.len() {
            let doc = self.buffer[self.pos].clone();
            self.pos += 1;
            Ok(Some(doc))
        } else if self.exhausted {
            Ok(None)
        } else {
            // Stub: would fetch more from context
            self.exhausted = true;
            Ok(None)
        }
    }

    pub fn close(&mut self) {
        self.exhausted = true;
        self.buffer.clear();
    }
}
