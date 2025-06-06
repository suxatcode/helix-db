use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use crate::helix_engine::types::GraphError;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use std::{
    net::SocketAddr,
    collections::HashMap,
    sync::{Arc, Mutex},
};
use crate::helix_runtime::AsyncRuntime;
use crate::helix_transport::Transport;

use crate::helix_gateway::{router::router::HelixRouter, thread_pool::thread_pool::ThreadPool};

pub struct ConnectionHandler<R, T>
where
    R: AsyncRuntime + Clone + Send + Sync + 'static,
    T: Transport,
{
    pub address: String,
    pub active_connections: Arc<Mutex<HashMap<String, ClientConnection>>>,
    pub thread_pool: ThreadPool<R, T::Stream>,
    pub runtime: R,
    pub transport: T,
}

pub struct ClientConnection {
    pub id: String,
    pub last_active: DateTime<Utc>,
    pub addr: SocketAddr,
}

impl<R, T> ConnectionHandler<R, T>
where
    R: AsyncRuntime + Clone + Send + Sync + 'static,
    T: Transport,
{
    pub fn new(
        address: &str,
        graph: Arc<HelixGraphEngine>,
        size: usize,
        router: HelixRouter,
        runtime: R,
        transport: T,
    ) -> Result<Self, GraphError> {
        Ok(Self {
            address: address.to_string(),
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            thread_pool: ThreadPool::new(size, graph, Arc::new(router), runtime.clone())?,
            runtime,
            transport,
        })
    }

    pub async fn accept_conns(&self) -> Result<<R as AsyncRuntime>::JoinHandle<()>, GraphError> {
        let listener = self
            .transport
            .bind(&self.address)
            .await
            .map_err(|e| {
                eprintln!("Failed to bind to address {}: {}", self.address, e);
                GraphError::GraphConnectionError("Failed to bind to address".to_string(), e)
            })?;

        // Log binding success to stderr since stdout might be buffered

        let active_connections = Arc::clone(&self.active_connections);
        let thread_pool_sender = self.thread_pool.sender.clone();
        let runtime = self.runtime.clone();
        let transport = self.transport.clone();

        let handle = runtime.spawn(async move {
            let listener = listener;
            loop {
                match transport.accept(&listener).await {
                    Ok((stream, addr)) => {

                        // Create a client connection record
                        let client_id = Uuid::new_v4().to_string();
                        let client = ClientConnection {
                            id: client_id.clone(),
                            last_active: Utc::now(),
                            addr,
                        };

                        // Add to active connections
                        active_connections
                            .lock()
                            .unwrap()
                            .insert(client_id.clone(), client);

                        // Send to thread pool
                        match thread_pool_sender.send_async(stream).await {
                            Ok(_) => (),
                            Err(e) => {
                                eprintln!("Error sending connection {} to thread pool: {}", client_id, e);
                                active_connections.lock().unwrap().remove(&client_id);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error accepting connection: {}", e);
                    }
                }
            }
        });

        Ok(handle)
    }
}
