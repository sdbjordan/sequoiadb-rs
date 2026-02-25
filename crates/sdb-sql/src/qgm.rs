use crate::ast::SqlStatement;
use sdb_common::Result;
use sdb_opt::QueryPlan;

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
