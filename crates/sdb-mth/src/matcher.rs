use sdb_bson::Document;
use sdb_common::Result;

/// Query matcher — evaluates whether a document matches a query condition.
/// Corresponds to the original mthMatcher class.
pub struct Matcher {
    condition: Document,
}

impl Matcher {
    pub fn new(condition: Document) -> Result<Self> {
        Ok(Self { condition })
    }

    /// Returns true if the document matches the condition.
    pub fn matches(&self, _doc: &Document) -> Result<bool> {
        // Stub: match all
        let _ = &self.condition;
        Ok(true)
    }

    /// Returns true if this matcher matches everything (empty condition).
    pub fn is_match_all(&self) -> bool {
        self.condition.is_empty()
    }
}
