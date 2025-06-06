pub mod tokio_transport;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};

/// Abstraction over network transport for HelixDB.
///
/// The transport trait allows the gateway to be decoupled from a
/// concrete networking stack so that simulation tests can provide a
/// deterministic in-memory transport.
pub trait Transport {
    type Listener: Send + Sync + 'static;
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// Bind a listener to the provided address.
    fn bind<'a>(addr: &'a str) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Listener>> + Send + 'a>>;

    /// Accept the next incoming connection from a listener.
    fn accept<'a>(listener: &'a Self::Listener) -> Pin<Box<dyn Future<Output = std::io::Result<(Self::Stream, SocketAddr)>> + Send + 'a>>;

    /// Connect to a remote address returning a stream.
    fn connect<'a>(addr: &'a str) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send + 'a>>;
}
