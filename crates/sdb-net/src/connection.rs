use sdb_common::{NodeAddress, Result};
use sdb_msg::MsgHeader;
use tokio::net::TcpStream;

/// A single TCP connection to a remote node.
pub struct Connection {
    _stream: TcpStream,
    pub remote: NodeAddress,
}

impl Connection {
    /// Send a message over this connection.
    pub async fn send(&self, _header: &MsgHeader, _payload: &[u8]) -> Result<()> {
        // Stub
        Ok(())
    }

    /// Receive a message from this connection.
    pub async fn recv(&self) -> Result<(MsgHeader, Vec<u8>)> {
        // Stub
        Err(sdb_common::SdbError::NetworkClose)
    }
}
