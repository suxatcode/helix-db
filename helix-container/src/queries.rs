use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::helix_engine::storage_core::storage_methods::BasicStorageMethods;
use helixdb::helix_engine::vector_core::vector::HVector;
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
    let file_path = "/home/ec2-user/com-friendster.ungraph.txt";
    let file = File::open(file_path).unwrap();
    let reader = BufReader::new(file);

    let db = Arc::clone(&input.graph.storage);

    let mut line_count = 0;

    let mut nodes = HashSet::with_capacity(65_000_000);
    let mut edges = HashSet::with_capacity(1_600_000_000);
    for line in reader.lines() {
        if line_count % 1_000_000 == 0 {
            println!("Processed {} lines", line_count);
        }
        line_count += 1;

        let line = line.unwrap();
        // Skip comments and empty lines
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

        nodes.insert(from_id);
        nodes.insert(to_id);
        edges.insert((from_id, to_id));
    }

    let mut txn = db.graph_env.write_txn().unwrap();
    let len = nodes.len();
    // if ids dont exist, create them
    for node in nodes {
        if node % 1_000_000 == 0 {
            println!("Added {} nodes", node);
        }

        db.create_node_(&mut txn, "", props! {}, None, Some(node))
            .unwrap();
    }
    println!("Added {} nodes", len);
    txn.commit().unwrap();

    for i in 1..=1000 {
        let start = edges.len() / 1000 * (i - 1);
        let end = edges.len() / 1000 * i;
        let mut txn = db.graph_env.write_txn().unwrap();
        for (from_id, to_id) in edges.iter().skip(start).take(end - start) {
            db.create_edge_(&mut txn, "knows", *from_id, *to_id, props! {})
                .unwrap();
        }
        txn.commit().unwrap();
        println!("Added {} edges", end - start);
    }
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
