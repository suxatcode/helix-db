use std::{collections::HashMap, sync::Arc};

use chrono::Utc;
use helix_engine::{graph_core::graph_core::HelixGraphEngine, props, storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods}};
use helix_gateway::{
    router::router::{HandlerFn, HandlerSubmission},
    GatewayOpts, HelixGateway,
};
use inventory;
use rand::Rng;
pub mod traversals;

fn main() {
    let path = format!("../graph_data/{}", Utc::now());
    let graph = HelixGraphEngine::new(path.as_str()).unwrap();
    create_test_graph(&graph.storage, 100, 10);
    let routes = HashMap::from_iter(
        inventory::iter::<HandlerSubmission>
            .into_iter()
            .map(|submission| {
                // get the handler from the submission
                let handler = &submission.0;

                // create a new handler function that wraps the collected basic handler function
                let func: HandlerFn = Arc::new(move |input, response| (handler.func)(input, response));

                // return tuple of method, path, and handler function
                (("get".to_ascii_uppercase().to_string(), format!("/{}", handler.name.to_string())), func)
            })
            .collect::<Vec<((String, String), HandlerFn)>>(),
    );
    let gateway = HelixGateway::new(
        "127.0.0.1:1234",
        graph,
        GatewayOpts::DEFAULT_POOL_SIZE,
        Some(routes),
    );

    // start server
    let _ = gateway.connection_handler.accept_conns().join().unwrap();
}

fn create_test_graph(storage: &HelixGraphStorage, size: usize, edges_per_node: usize) {
    let mut node_ids = Vec::with_capacity(size);

    for _ in 0..size {
        let node = storage.create_node("person", props!()).unwrap();
        node_ids.push(node.id);
    }

    let mut rng = rand::thread_rng();
    for from_id in &node_ids {
        for _ in 0..edges_per_node {
            let to_index = rng.gen_range(0..size);
            let to_id = &node_ids[to_index];
        
            if from_id != to_id {
                storage
                    .create_edge("knows", from_id, to_id, props!())
                    .unwrap();
            }
        }
    }
}
