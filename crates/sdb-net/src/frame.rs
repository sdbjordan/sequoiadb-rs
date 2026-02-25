use sdb_common::Result;
use crate::handler::MessageHandler;
use std::sync::Arc;

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
        tracing::info!("NetFrame listening on {}", self.bind_addr);
        // Stub: actual TCP accept loop will go here
        Ok(())
    }

    /// Gracefully shut down the event loop.
    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!("NetFrame shutting down");
        Ok(())
    }
}
