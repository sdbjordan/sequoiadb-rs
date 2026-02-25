use sdb_bson::Document;
use sdb_common::Result;

/// Client-side cursor for iterating query results.
pub struct ClientCursor {
    buffer: Vec<Document>,
    pos: usize,
    closed: bool,
}

impl ClientCursor {
    pub fn empty() -> Self {
        Self {
            buffer: Vec::new(),
            pos: 0,
            closed: false,
        }
    }

    pub async fn next(&mut self) -> Result<Option<Document>> {
        if self.closed {
            return Ok(None);
        }
        if self.pos < self.buffer.len() {
            let doc = self.buffer[self.pos].clone();
            self.pos += 1;
            Ok(Some(doc))
        } else {
            // Stub: would request more from server
            self.closed = true;
            Ok(None)
        }
    }

    pub fn close(&mut self) {
        self.closed = true;
    }
}
