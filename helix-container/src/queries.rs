use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Seek};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use get_routes::handler;
use helixdb::helix_engine::graph_core::ops::source::bulk_add_e::BulkAddEAdapter;
use helixdb::helix_engine::graph_core::ops::source::bulk_add_n::BulkAddNAdapter;
use helixdb::helix_engine::storage_core::storage_core::HelixGraphStorage;
use helixdb::helix_engine::storage_core::storage_methods::BasicStorageMethods;
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::protocol::items::{Edge, Node};
use helixdb::{
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::AddEAdapter, add_n::AddNAdapter, e::EAdapter, e_from_id::EFromId,
            e_from_types::EFromTypes, n::NAdapter, n_from_id::NFromIdAdapter,
            n_from_types::NFromTypesAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, drop::DropAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, range::RangeAdapter, update::Update,
        },
        vectors::{insert::InsertVAdapter, search::SearchVAdapter},
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::remapping::ResponseRemapping,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::{
        filterable::Filterable, remapping::Remapping, return_values::ReturnValue, value::Value,
    },
};
use sonic_rs::{Deserialize, Serialize};

#[handler]
pub fn bulk_loader(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    // line by line from ~/com-friendster.ungraph.txt
    let file_path = "/Users/xav/com-friendster.ungraph.txt";
    let file = File::open(file_path).unwrap();
    let file_size = file.metadata()?.len();
    let num_threads = 16;
    let chunk_size = file_size / num_threads as u64;
    let db = Arc::clone(&input.graph.storage);

    // Shared data structures
    let nodes = Arc::new(Mutex::new(HashSet::with_capacity(65_000_000)));
    let edges = Arc::new(Mutex::new(Vec::with_capacity(1_600_000_000)));

    // Create thread handles
    let mut handles = Vec::with_capacity(num_threads);

    let line_count = Arc::new(AtomicU64::new(0));

    for i in 0..num_threads {
        let start_pos = i as u64 * chunk_size;
        let end_pos = if i == num_threads - 1 {
            file_size
        } else {
            (i as u64 + 1) * chunk_size
        };

        let file_path = file_path.to_string();
        let nodes_clone: Arc<Mutex<HashSet<u128>>> = Arc::clone(&nodes);
        let edges_clone: Arc<Mutex<Vec<(u128, u128)>>> = Arc::clone(&edges);
        let db_clone = Arc::clone(&db);
        let line_count = Arc::clone(&line_count);

        let handle = thread::spawn(move || {
            let mut file = File::open(&file_path).unwrap();
            file.seek(std::io::SeekFrom::Start(start_pos)).unwrap();

            let reader = BufReader::new(file);
            let mut local_nodes = HashSet::new();
            let mut local_edges = Vec::new();

            let mut bytes_read = 0;

            for line in reader.lines() {
                let line = line.unwrap();
                bytes_read += line.len() as u64 + 1; // +1 for newline

                // Skip the first line if it's not the first thread (might be partial)
                if i > 0 && line_count.load(Ordering::Relaxed) == 0 {
                    line_count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Process line
                if line.starts_with('#') || line.trim().is_empty() {
                    continue;
                }

                // Split line into from_id and to_id
                let ids: Vec<&str> = line.split_whitespace().collect();
                if ids.len() != 2 {
                    continue;
                }

                let from_id = ids[0].parse::<u128>().unwrap();
                let to_id = ids[1].parse::<u128>().unwrap();

                local_nodes.insert(from_id);
                if from_id != to_id {
                    local_nodes.insert(to_id);
                }
                local_edges.push((from_id, to_id));

                line_count.fetch_add(1, Ordering::Relaxed);

                // Stop if we've exceeded our chunk size (unless it's the last thread)
                if i < num_threads - 1 && bytes_read >= chunk_size {
                    break;
                }
                if line_count.load(Ordering::Relaxed) % 1_000_000 == 0 {
                    println!(
                        "Thread {} processed {} lines",
                        i,
                        line_count.load(Ordering::Relaxed)
                    );
                }
            }

            // Merge local results into shared data structures
            let mut nodes = nodes_clone.lock().unwrap();
            nodes.extend(local_nodes);

            let mut edges = edges_clone.lock().unwrap();
            edges.extend(local_edges);

            println!(
                "Thread {} processed {} lines",
                i,
                line_count.load(Ordering::Relaxed)
            );
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Now process all the collected data
    let mut nodes = Arc::try_unwrap(nodes)
        .unwrap()
        .into_inner()
        .unwrap()
        .into_iter()
        .map(|n| Node {
            id: n,
            label: "user".to_string(),
            properties: HashMap::new(),
        })
        .collect::<Vec<_>>();
    let mut edges = Arc::try_unwrap(edges)
        .unwrap()
        .into_inner()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(id, (from_node, to_node))| Edge {
            id: id.try_into().unwrap(),
            label: "user".to_string(),
            properties: HashMap::new(),
            from_node,
            to_node,
        })
        .collect::<Vec<_>>();

    let mut txn = db.graph_env.write_txn().unwrap();
    let len = nodes.len();
    // if ids dont exist, create them
    let n = G::new_mut(Arc::clone(&db), &mut txn)
        .bulk_add_n(nodes.as_mut_slice(), None)
        .count();

    let e = G::new_mut(Arc::clone(&db), &mut txn)
    .bulk_add_e(edges.as_mut_slice(), false, 1_000_000)
    .count();

    Ok(())
}

#[handler]
pub fn one_hop_friends(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    #[derive(Serialize, Deserialize)]
    struct OneHopFriendsRequest {
        start_id: u128,
    }

    let data: OneHopFriendsRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tr = G::new(Arc::clone(&db), &txn);
    let friends_count = tr.n_from_id(&data.start_id).out("knows").dedup().count();

    response.body = serde_json::to_vec(&friends_count).unwrap();
    Ok(())
}

#[handler]
pub fn three_hop_friends(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    #[derive(Serialize, Deserialize)]
    struct ThreeHopFriendsRequest {
        start_id: u128,
    }

    let data: ThreeHopFriendsRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tr = G::new(Arc::clone(&db), &txn);
    let friends_count = tr
        .n_from_id(&data.start_id)
        .out("knows")
        .out("knows")
        .out("knows")
        .dedup()
        .count();

    response.body = serde_json::to_vec(&friends_count).unwrap();
    Ok(())
}

#[handler]
pub fn six_hop_friends(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    #[derive(Serialize, Deserialize)]
    struct SixHopFriendsRequest {
        start_id: u128,
    }

    let data: SixHopFriendsRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tr = G::new(Arc::clone(&db), &txn);
    let friends_count = tr
        .n_from_id(&data.start_id)
        .out("knows")
        .out("knows")
        .out("knows")
        .out("knows")
        .out("knows")
        .out("knows")
        .dedup()
        .count();

    response.body = serde_json::to_vec(&friends_count).unwrap();
    Ok(())
}
