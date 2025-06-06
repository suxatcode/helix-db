use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};

pub mod tcp_transport;

/// Abstraction over network transport used by the gateway.
/// Provides operations to bind listeners, accept connections and connect to peers.
pub trait Transport: Clone + Send + Sync + 'static {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    type Listener: Send + Sync + 'static;

    fn bind(&self, addr: &str) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Listener>> + Send>>;
    fn accept(&self, listener: &Self::Listener) -> Pin<Box<dyn Future<Output = std::io::Result<(Self::Stream, SocketAddr)>> + Send>>;
    fn connect(&self, addr: &str) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>>;
}
