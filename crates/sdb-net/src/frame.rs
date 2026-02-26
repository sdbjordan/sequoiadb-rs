use crate::connection::Connection;
use crate::handler::MessageHandler;
use sdb_common::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;

#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;

/// Network event loop — accepts connections and dispatches messages.
pub struct NetFrame {
    bind_addr: String,
    handler: Option<Arc<dyn MessageHandler>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    #[cfg(feature = "tls")]
    tls_acceptor: Option<TlsAcceptor>,
}

impl NetFrame {
    pub fn new(bind_addr: impl Into<String>) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            bind_addr: bind_addr.into(),
            handler: None,
            shutdown_tx,
            shutdown_rx,
            #[cfg(feature = "tls")]
            tls_acceptor: None,
        }
    }

    pub fn set_handler(&mut self, handler: Arc<dyn MessageHandler>) {
        self.handler = Some(handler);
    }

    /// Get a shutdown sender that can signal the event loop to stop.
    pub fn shutdown_sender(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    /// Set a TLS acceptor for incoming connections.
    #[cfg(feature = "tls")]
    pub fn set_tls_acceptor(&mut self, acceptor: TlsAcceptor) {
        self.tls_acceptor = Some(acceptor);
    }

    /// Start the event loop. Listens for connections and dispatches messages.
    /// Exits gracefully when shutdown signal is received.
    pub async fn run(&self) -> Result<()> {
        let handler = self
            .handler
            .clone()
            .expect("MessageHandler must be set before calling run()");

        let listener = TcpListener::bind(&self.bind_addr)
            .await
            .map_err(|_| sdb_common::SdbError::NetworkError)?;

        tracing::info!("NetFrame listening on {}", self.bind_addr);

        #[cfg(feature = "tls")]
        let tls_acceptor = self.tls_acceptor.clone();

        let mut shutdown_rx = self.shutdown_rx.clone();
        let active_conns = Arc::new(std::sync::atomic::AtomicU64::new(0));

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("NetFrame received shutdown signal, draining connections...");
                        // Wait for active connections to drain (up to 5s)
                        let drain_start = std::time::Instant::now();
                        while active_conns.load(std::sync::atomic::Ordering::Relaxed) > 0
                            && drain_start.elapsed() < std::time::Duration::from_secs(5)
                        {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                        let remaining = active_conns.load(std::sync::atomic::Ordering::Relaxed);
                        if remaining > 0 {
                            tracing::warn!("Shutdown with {} connections still active", remaining);
                        }
                        tracing::info!("NetFrame shutdown complete");
                        return Ok(());
                    }
                }
                accept_result = listener.accept() => {
                    let (stream, addr) = accept_result
                        .map_err(|_| sdb_common::SdbError::NetworkError)?;

                    let handler = handler.clone();
                    let conns = active_conns.clone();
                    conns.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    #[cfg(feature = "tls")]
                    let tls_acceptor = tls_acceptor.clone();

                    tokio::spawn(async move {
                        // If TLS is configured, wrap the stream
                        #[cfg(feature = "tls")]
                        let mut conn = if let Some(ref acceptor) = tls_acceptor {
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => Connection::new_tls_server(tls_stream, addr),
                                Err(e) => {
                                    tracing::warn!("TLS handshake failed for {}: {}", addr, e);
                                    conns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                    return;
                                }
                            }
                        } else {
                            Connection::new(stream, addr)
                        };

                        #[cfg(not(feature = "tls"))]
                        let mut conn = Connection::new(stream, addr);

                        tracing::debug!("Connection accepted from {}", addr);

                        if let Err(e) = handler.on_connect(&conn).await {
                            tracing::warn!("on_connect error for {}: {}", addr, e);
                            conns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
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
                        conns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::debug!("Connection closed from {}", addr);
                    });
                }
            }
        }
    }

    /// Gracefully shut down the event loop.
    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!("NetFrame shutting down");
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }
}
