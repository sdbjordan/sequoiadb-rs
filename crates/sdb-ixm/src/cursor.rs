use sdb_common::RecordId;

/// Cursor for iterating over index scan results.
pub struct IndexCursor {
    results: Vec<RecordId>,
    pos: usize,
}

impl IndexCursor {
    pub fn empty() -> Self {
        Self {
            results: Vec::new(),
            pos: 0,
        }
    }

    pub fn next(&mut self) -> Option<RecordId> {
        if self.pos < self.results.len() {
            let rid = self.results[self.pos];
            self.pos += 1;
            Some(rid)
        } else {
            None
        }
    }

    pub fn has_more(&self) -> bool {
        self.pos < self.results.len()
    }
}
