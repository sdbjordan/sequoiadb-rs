use sdb_opt::QueryPlan;
use sdb_common::Result;
use crate::ast::SqlStatement;

/// Query Graph Model — converts SQL AST to query plans.
pub struct QueryGraph;

impl QueryGraph {
    pub fn new() -> Self {
        Self
    }

    /// Convert a SQL statement into a query plan.
    pub fn to_plan(&self, _stmt: &SqlStatement) -> Result<QueryPlan> {
        // Stub
        Err(sdb_common::SdbError::InvalidArg)
    }
}

impl Default for QueryGraph {
    fn default() -> Self {
        Self::new()
    }
}
