use crate::connection::Connection;
use crate::handler::MessageHandler;
use sdb_common::Result;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Network event loop — accepts connections and dispatches messages.
pub struct NetFrame {
    bind_addr: String,
    handler: Option<Arc<dyn MessageHandler>>,
}

impl NetFrame {
    pub fn new(bind_addr: impl Into<String>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            handler: None,
        }
    }

    pub fn set_handler(&mut self, handler: Arc<dyn MessageHandler>) {
        self.handler = Some(handler);
    }

    /// Start the event loop. Listens for connections and dispatches messages.
    pub async fn run(&self) -> Result<()> {
        let handler = self
            .handler
            .clone()
            .expect("MessageHandler must be set before calling run()");

        let listener = TcpListener::bind(&self.bind_addr)
            .await
            .map_err(|_| sdb_common::SdbError::NetworkError)?;

        tracing::info!("NetFrame listening on {}", self.bind_addr);

        loop {
            let (stream, addr) = listener
                .accept()
                .await
                .map_err(|_| sdb_common::SdbError::NetworkError)?;

            let handler = handler.clone();
            tokio::spawn(async move {
                let mut conn = Connection::new(stream, addr);
                tracing::debug!("Connection accepted from {}", addr);

                if let Err(e) = handler.on_connect(&conn).await {
                    tracing::warn!("on_connect error for {}: {}", addr, e);
                    return;
                }

                while let Ok((header, payload)) = conn.recv_msg().await {
                    if let Err(e) =
                        handler.on_message(&mut conn, header, &payload).await
                    {
                        tracing::debug!("on_message result for {}: {}", addr, e);
                        break;
                    }
                }

                if let Err(e) = handler.on_disconnect(&conn).await {
                    tracing::warn!("on_disconnect error for {}: {}", addr, e);
                }
                tracing::debug!("Connection closed from {}", addr);
            });
        }
    }

    /// Gracefully shut down the event loop.
    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!("NetFrame shutting down");
        Ok(())
    }
}
