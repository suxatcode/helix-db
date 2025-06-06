use super::Transport;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Default)]
pub struct TcpTransport;

impl Transport for TcpTransport {
    type Stream = TcpStream;
    type Listener = TcpListener;

    fn bind(&self, addr: &str) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Listener>> + Send>> {
        Box::pin(async move { TcpListener::bind(addr).await })
    }

    fn accept(&self, listener: &Self::Listener) -> Pin<Box<dyn Future<Output = std::io::Result<(Self::Stream, SocketAddr)>> + Send>> {
        Box::pin(async move { listener.accept().await })
    }

    fn connect(&self, addr: &str) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        Box::pin(async move { TcpStream::connect(addr).await })
    }
}
