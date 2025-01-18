use flume::{Receiver, Sender};
use helix_engine::graph_core::graph_core::HelixGraphEngine;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::router::router::HelixRouter;
use protocol::request::Request;
use protocol::response::Response;

pub struct Worker {
    pub id: usize,
    pub thread: thread::JoinHandle<()>,
    // pub reciever: Arc<Mutex<Receiver<TcpStream>>>,
}

impl Worker {
    fn new(
        id: usize,
        graph_access: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
        rx: Arc<Mutex<Receiver<TcpStream>>>,
    ) -> Arc<Worker> {
        Arc::new(Worker {
            id,
            thread: thread::spawn(move || loop {
                let mut conn = rx.lock().unwrap().recv().unwrap(); // TODO: Handle error
                let request = Request::from_stream(&mut conn).unwrap(); // TODO: Handle Error
                let mut response = Response::new();
                // println!("Worker {} handling request: {:?}", id, request);
                if let Err(e) = router.handle(Arc::clone(&graph_access), request, &mut response) {
                    eprintln!("Error handling request: {:?}", e);
                    response.status = 500;
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
            }),
        })
    }
}

pub struct ThreadPool {
    pub sender: Sender<TcpStream>,
    pub num_unused_workers: Mutex<usize>,
    pub num_used_workers: Mutex<usize>,
    pub workers: Mutex<Vec<Arc<Worker>>>,
}

impl ThreadPool {
    pub fn new(size: usize, graph: Arc<HelixGraphEngine>, router: Arc<HelixRouter>) -> Self {
        assert!(
            size > 0,
            "Expected number of threads in thread pool to be more than 0, got {}",
            size
        );
        let mut workers = Vec::with_capacity(size);
        let (tx, rx) = flume::unbounded::<TcpStream>();

        let reciever = Arc::new(Mutex::new(rx));
        for id in 0..size {
            workers.push(Worker::new(
                id,
                Arc::clone(&graph),
                Arc::clone(&router),
                Arc::clone(&reciever),
            ));
        }
        ThreadPool {
            sender: tx,
            num_unused_workers: Mutex::new(workers.len()),
            num_used_workers: Mutex::new(0),
            // used_workers: Mutex::new(Vec::with_capacity(workers.len())),
            workers: Mutex::new(workers),
        }
    }
}
