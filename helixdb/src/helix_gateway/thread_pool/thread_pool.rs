use chrono::format;
use flume::{Receiver, Sender};
use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::helix_gateway::router::router::{HelixRouter, RouterError};
use crate::helix_gateway::gateway::GatewayOpts;
use crate::protocol::request::Request;
use crate::protocol::response::Response;

pub struct Worker {
    pub id: usize,
    pub thread: thread::JoinHandle<Result<(), RouterError>>, // pub reciever: Arc<Mutex<Receiver<TcpStream>>>,
}

impl Worker {
    fn new(
        id: usize,
        graph_access: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
        rx: Arc<Mutex<Receiver<TcpStream>>>,
    ) -> Result<Arc<Worker>, RouterError> {
        let thread: thread::JoinHandle<Result<(), RouterError>> =
            thread::spawn(move || -> Result<(), RouterError> {
                loop {
                    let mut conn = rx.lock().unwrap().recv().unwrap(); // TODO: Handle error
                    let request = Request::from_stream(&mut conn)?; // TODO: Handle Error
                    let mut response = Response::new();

                    if let Err(e) = router.handle(Arc::clone(&graph_access), request, &mut response)
                    {
                        eprintln!("Error handling request: {:?}", e);
                        response.status = 500;
                        response.body = format!("\n{:?}", e).into_bytes();
                    }

                    if let Err(e) = response.send(&mut conn) {
                        eprintln!("Error sending response: {:?}", e);
                        match e.kind() {
                            std::io::ErrorKind::BrokenPipe => {
                                eprintln!("Client disconnected before response could be sent");
                            }
                            std::io::ErrorKind::ConnectionReset => {
                                eprintln!("Connection was reset by peer");
                            }
                            _ => {
                                eprintln!("Unexpected error type: {:?}", e);
                            }
                        }
                    }
                }
            });
        Ok(Arc::new(Worker { id, thread }))
    }
}

pub struct ThreadPool {
    pub sender: Sender<TcpStream>,
    pub num_unused_workers: Mutex<usize>,
    pub num_used_workers: Mutex<usize>,
    pub workers: Mutex<Vec<Arc<Worker>>>,
}

impl ThreadPool {
    pub fn new(
        size: usize,
        graph: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
    ) -> Result<ThreadPool, RouterError> {
        assert!(
            size > 0,
            "Expected number of threads in thread pool to be more than 0, got {}",
            size
        );
        let mut workers = Vec::with_capacity(size);
        let (tx, rx) = flume::bounded::<TcpStream>(GatewayOpts::DEFAULT_POOL_SIZE);

        let reciever = Arc::new(Mutex::new(rx));
        for id in 0..size {
            workers.push(Worker::new(
                id,
                Arc::clone(&graph),
                Arc::clone(&router),
                Arc::clone(&reciever),
            )?);
        }
        Ok(ThreadPool {
            sender: tx,
            num_unused_workers: Mutex::new(workers.len()),
            num_used_workers: Mutex::new(0),
            // used_workers: Mutex::new(Vec::with_capacity(workers.len())),
            workers: Mutex::new(workers),
        })
    }
}
