use sdb_bson::Document;

/// Execution context — holds state for an in-progress query.
/// Corresponds to rtnContext in the original engine.
pub struct Context {
    pub context_id: i64,
    pub collection: String,
    pub eof: bool,
    buffer: Vec<Document>,
}

impl Context {
    pub fn new(context_id: i64, collection: String) -> Self {
        Self {
            context_id,
            collection,
            eof: false,
            buffer: Vec::new(),
        }
    }

    pub fn append(&mut self, doc: Document) {
        self.buffer.push(doc);
    }

    pub fn get_more(&mut self, count: usize) -> Vec<Document> {
        let n = count.min(self.buffer.len());
        self.buffer.drain(..n).collect()
    }
}
