use sdb_common::{Result, SdbError};
use sdb_msg::header::MsgHeader;
use sdb_msg::reply::MsgOpReply;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

/// A stream that is either plain TCP or TLS-wrapped.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
    #[cfg(feature = "tls")]
    TlsServer(tokio_rustls::server::TlsStream<TcpStream>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(ref mut s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(ref mut s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::TlsServer(ref mut s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(ref mut s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(ref mut s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::TlsServer(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(ref mut s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(ref mut s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::TlsServer(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(ref mut s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(ref mut s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::TlsServer(ref mut s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// A single connection to a remote client/node.
pub struct Connection {
    stream: MaybeTlsStream,
    pub addr: SocketAddr,
}

impl Connection {
    pub fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self {
            stream: MaybeTlsStream::Plain(stream),
            addr,
        }
    }

    /// Create a connection wrapping a MaybeTlsStream.
    pub fn new_from_stream(stream: MaybeTlsStream, addr: SocketAddr) -> Self {
        Self { stream, addr }
    }

    /// Create a connection from a TLS server stream.
    #[cfg(feature = "tls")]
    pub fn new_tls_server(
        tls_stream: tokio_rustls::server::TlsStream<TcpStream>,
        addr: SocketAddr,
    ) -> Self {
        Self {
            stream: MaybeTlsStream::TlsServer(tls_stream),
            addr,
        }
    }

    /// Send a reply message over the connection.
    pub async fn send_reply(&mut self, reply: &MsgOpReply) -> Result<()> {
        let bytes = reply.encode();
        self.stream
            .write_all(&bytes)
            .await
            .map_err(|_| SdbError::NetworkError)?;
        self.stream
            .flush()
            .await
            .map_err(|_| SdbError::NetworkError)?;
        Ok(())
    }

    /// Receive a message: first reads 4-byte msg_len, then reads the remaining bytes.
    /// Returns the decoded header and the payload (everything after the 52-byte header).
    pub async fn recv_msg(&mut self) -> Result<(MsgHeader, Vec<u8>)> {
        // Read the first 4 bytes to get msg_len
        let mut len_buf = [0u8; 4];
        self.stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|_| SdbError::NetworkClose)?;
        let msg_len = i32::from_le_bytes(len_buf) as usize;

        if msg_len < MsgHeader::SIZE {
            return Err(SdbError::InvalidArg);
        }

        // Read the rest of the message (msg_len - 4 bytes already read)
        let mut full_buf = Vec::with_capacity(msg_len);
        full_buf.extend_from_slice(&len_buf);
        full_buf.resize(msg_len, 0);
        self.stream
            .read_exact(&mut full_buf[4..])
            .await
            .map_err(|_| SdbError::NetworkClose)?;

        let header = MsgHeader::decode(&full_buf)?;
        let payload = full_buf[MsgHeader::SIZE..].to_vec();

        Ok((header, payload))
    }
}
