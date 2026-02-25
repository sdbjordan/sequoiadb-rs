use crate::connection::Connection;
use async_trait::async_trait;
use sdb_common::Result;
use sdb_msg::MsgHeader;

/// Trait for handling incoming messages on a connection.
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// Called when a complete message is received.
    async fn on_message(
        &self,
        conn: &mut Connection,
        header: MsgHeader,
        payload: &[u8],
    ) -> Result<()>;

    /// Called when a new connection is accepted.
    async fn on_connect(&self, _conn: &Connection) -> Result<()> {
        Ok(())
    }

    /// Called when a connection is closed.
    async fn on_disconnect(&self, _conn: &Connection) -> Result<()> {
        Ok(())
    }
}
