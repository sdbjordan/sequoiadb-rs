use async_trait::async_trait;
use sdb_common::Result;
use sdb_msg::MsgHeader;
use crate::connection::Connection;

/// Trait for handling incoming messages.
#[async_trait]
pub trait MessageHandler: Send + Sync {
    async fn on_message(&self, conn: &Connection, header: MsgHeader, payload: &[u8]) -> Result<()>;
    async fn on_connect(&self, _conn: &Connection) -> Result<()> {
        Ok(())
    }
    async fn on_disconnect(&self, _conn: &Connection) -> Result<()> {
        Ok(())
    }
}
