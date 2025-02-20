use helixdb::helix_engine::{
    graph_core::graph_core::{HelixGraphEngine, HelixGraphEngineOpts},
    storage_core::{
        storage_core::HelixGraphStorage,
        storage_methods::{DBMethods, StorageMethods},
    },
};
use helixdb::helix_gateway::{
    gateway::{GatewayOpts, HelixGateway},
    router::router::{HandlerFn, HandlerSubmission},
};
use helixdb::props;
use inventory;
use rand::Rng;
use std::{collections::HashMap, ops::Deref, sync::Arc, time::Instant};

mod ivba_traversals;
mod traversals;
mod queries;

fn main() {
    let path = match std::env::var("HELIX_DATA_DIR") {
        Ok(val) => std::path::PathBuf::from(val).join(".helix/user"),
        Err(_) => {
            println!("HELIX_DATA_DIR not set, using default");
            let home = dirs::home_dir().expect("Could not retrieve home directory");
            home.join(".helix/user")
        }
    };
    let path_str = path.to_str().expect("Could not convert path to string");
    let opts = HelixGraphEngineOpts {
        path: path_str.to_string(),
        secondary_indices: Some(vec!["username".to_string(), "x_id".to_string()]),
    };
    let graph = Arc::new(HelixGraphEngine::new(opts).unwrap());
    // create_test_graph(Arc::clone(&graph), 15000, 250);

    // generates routes from handler proc macro
    println!("Starting route collection...");
    let submissions: Vec<_> = inventory::iter::<HandlerSubmission>.into_iter().collect();
    println!("Found {} submissions", submissions.len());

    let routes = HashMap::from_iter(
        submissions
            .into_iter()
            .map(|submission| {
                println!("Processing submission for handler: {}", submission.0.name);
                let handler = &submission.0;
                let func: HandlerFn =
                    Arc::new(move |input, response| (handler.func)(input, response));
                (
                    (
                        "post".to_ascii_uppercase().to_string(),
                        format!("/{}", handler.name.to_string()),
                    ),
                    func,
                )
            })
            .collect::<Vec<((String, String), HandlerFn)>>(),
    );

    println!("Routes: {:?}", routes.keys());
    // create gateway
    let gateway = HelixGateway::new(
        "0.0.0.0:6969",
        graph,
        GatewayOpts::DEFAULT_POOL_SIZE,
        Some(routes),
    );

    // start server
    let _ = gateway.connection_handler.accept_conns().join().unwrap(); // TODO handle error causes panic
}

fn create_test_graph(graph: Arc<HelixGraphEngine>, size: usize, edges_per_node: usize) {
    let now = Instant::now();
    let storage = &graph.storage; //.lock().unwrap();
    let mut node_ids = Vec::with_capacity(size + 1);
    let mut txn = storage.graph_env.write_txn().unwrap();
    let node = storage
        .create_node(
            &mut txn,
            "user",
            props! { "username" => "Xav".to_string()},
            None,
        )
        .unwrap();
    println!("Node: {:?}", node);
    node_ids.push(node.id);
    for _ in 0..size {
        let node = storage
            .create_node(
                &mut txn,
                "user",
                props! { "username" => generate_random_name()},
                None,
            )
            .unwrap();
        node_ids.push(node.id);
    }

    let mut rng = rand::thread_rng();
    for from_id in &node_ids {
        for _ in 0..edges_per_node {
            let to_index = rng.gen_range(0..=size);
            let to_id = &node_ids[to_index];

            if from_id != to_id {
                storage
                    .create_edge(&mut txn, "follows", from_id, to_id, props!())
                    .unwrap();
                storage
                    .create_edge(&mut txn, "follows", to_id, from_id, props!())
                    .unwrap();
            }
        }
    }
    txn.commit().unwrap();
    let elapsed = now.elapsed();
    println!("Graph creation took: {:?}", elapsed);
}

use rand::seq::SliceRandom;

pub fn generate_random_name() -> String {
    let prefixes = ["Ze", "Xa", "Ky", "Ja", "Lu", "Ri", "So", "Ma", "De", "Vi"];
    let middles = ["ra", "li", "na", "ta", "ri", "ko", "mi", "sa", "do", ""];
    let suffixes = ["x", "n", "th", "ra", "na", "ka", "ta", "ix", "sa", ""];

    let mut rng = rand::thread_rng();

    // Randomly decide if we want to use a middle part
    let use_middle = rng.gen_bool(0.7); // 70% chance to use middle

    let prefix = prefixes.choose(&mut rng).unwrap();
    let middle = if use_middle {
        middles.choose(&mut rng).unwrap()
    } else {
        ""
    };
    let suffix = suffixes.choose(&mut rng).unwrap();

    format!("{}{}{}", prefix, middle, suffix)
}
