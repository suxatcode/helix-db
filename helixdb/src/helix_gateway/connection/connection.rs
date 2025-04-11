use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use crate::helix_engine::types::GraphError;
use crate::protocol::response::Response;
use chrono::{DateTime, Utc};
use socket2::{Domain, Socket, Type};
use std::net::SocketAddr;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{
    collections::HashMap,
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};
use uuid::Uuid;

use crate::helix_gateway::{router::router::HelixRouter, thread_pool::thread_pool::ThreadPool};



pub struct ConnectionHandler {
    pub listener: TcpListener,
    pub active_connections: Arc<Mutex<HashMap<String, ClientConnection>>>,
    pub thread_pool: ThreadPool,
}

pub struct ClientConnection {
    pub id: String,
    pub stream: TcpStream,
    pub last_active: DateTime<Utc>,
}

impl ConnectionHandler {
    pub fn new(
        address: &str,
        graph: Arc<HelixGraphEngine>,
        size: usize,
        router: HelixRouter,
    ) -> Result<Self, GraphError> {
        let addr: SocketAddr = address.parse()?;

        // Create the socket with socket2
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None).map_err(|e| {
            GraphError::GraphConnectionError("Failed to create socket".to_string(), e)
        })?;

        // Set socket options
        socket.set_recv_buffer_size(128 * 1024).map_err(|e| {
            GraphError::GraphConnectionError("Failed to set recv buffer".to_string(), e)
        })?;
        socket.set_send_buffer_size(128 * 1024).map_err(|e| {
            GraphError::GraphConnectionError("Failed to set send buffer".to_string(), e)
        })?;

        // Enable reuse
        socket.set_reuse_address(true).map_err(|e| {
            GraphError::GraphConnectionError("Failed to set reuse address".to_string(), e)
        })?;

        // Bind and listen
        socket
            .bind(&addr.into())
            .map_err(|e| GraphError::GraphConnectionError("Failed to bind".to_string(), e))?;
        socket
            .listen(1024)
            .map_err(|e| GraphError::GraphConnectionError("Failed to listen".to_string(), e))?;

        socket.set_nodelay(true)?; // Disables Nagle's algorithm, reducing latency
        socket.set_keepalive(true)?; // Detects dead connections
        socket.set_linger(Some(Duration::from_secs(5)))?;
        // Convert to std TcpListener
        let listener: TcpListener = socket.into();

        Ok(Self {
            listener,
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            thread_pool: ThreadPool::new(size, graph, Arc::new(router))?,
        })
    }

    pub fn accept_conns(&self) -> JoinHandle<Result<(), GraphError>> {
        let listener = self.listener.try_clone().unwrap();

        let active_connections = Arc::clone(&self.active_connections);
        let thread_pool_sender = self.thread_pool.sender.clone();
        thread::spawn(move || loop {
            let mut conn = match listener.accept() {
                Ok((conn, _)) => conn,
                Err(err) => {
                    // return Err(GraphError::GraphConnectionError(
                    //     "Failed to accept connection".to_string(),
                    //     err,
                    // ));
                    continue;
                }
            };

            // let conn_clone = conn.try_clone().unwrap();
            // let client = ClientConnection {
            //     id: Uuid::new_v4().to_string(),
            //     stream: conn_clone,
            //     last_active: Utc::now(),
            // };
            // // insert into hashmap
            // active_connections
            //     .lock()
            //     .unwrap()
            //     .insert(client.id.clone(), client);

            // pass conn to thread in thread pool via channel
            match thread_pool_sender.send_timeout(conn.try_clone().unwrap(), Duration::from_secs(5))
            {
                Ok(_) => (),
                Err(e) => {
                    let mut response = Response::new();
                    response.status = 503;
                    response.body = "Service Unavailable".as_bytes().to_vec();
                    response.send(&mut conn)?;
                    // Should also log the error and possibly clean up the connection from active_connections

                }
            }
        })
    }
}
