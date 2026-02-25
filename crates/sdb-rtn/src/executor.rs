use crate::cursor::Cursor;
use async_trait::async_trait;
use sdb_common::Result;
use sdb_opt::QueryPlan;

/// Query executor trait.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, plan: &QueryPlan) -> Result<Cursor>;
}

/// Default executor implementation (stub).
pub struct DefaultExecutor;

#[async_trait]
impl Executor for DefaultExecutor {
    async fn execute(&self, _plan: &QueryPlan) -> Result<Cursor> {
        // Stub: return empty cursor
        Ok(Cursor::new(0))
    }
}
