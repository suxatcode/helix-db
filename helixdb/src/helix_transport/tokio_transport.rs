use super::Transport;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

/// Tokio based transport implementation using TCP sockets.
#[derive(Clone, Default)]
pub struct TokioTransport;

impl Transport for TokioTransport {
    type Listener = TcpListener;
    type Stream = TcpStream;

    fn bind<'a>(addr: &'a str) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Listener>> + Send + 'a>> {
        Box::pin(async move { TcpListener::bind(addr).await })
    }

    fn accept<'a>(listener: &'a Self::Listener) -> Pin<Box<dyn Future<Output = std::io::Result<(Self::Stream, SocketAddr)>> + Send + 'a>> {
        Box::pin(async move { listener.accept().await })
    }

    fn connect<'a>(addr: &'a str) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send + 'a>> {
        Box::pin(async move { TcpStream::connect(addr).await })
    }
}
