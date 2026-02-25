use crate::ast::SqlStatement;
use sdb_common::Result;

/// SQL parser — converts SQL text to AST.
pub struct SqlParser;

impl SqlParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse(&self, _sql: &str) -> Result<SqlStatement> {
        // Stub
        Err(sdb_common::SdbError::InvalidArg)
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}
