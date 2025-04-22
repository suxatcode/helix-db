use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{
    node_matches,
    props,
    helix_engine::graph_core::traversal::TraversalBuilder,
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        vectors::{ insert::InsertVAdapter, search::SearchVAdapter},
        source::{add_e::AddEAdapter, add_n::AddNAdapter, e::EAdapter, e_from_id::EFromId, e_from_types::EFromTypes, n::NAdapter, n_from_id::NFromId, n_from_types::NFromTypes},
        tr_val::TraversalVal,
        util::{dedup::DedupAdapter, drop::DropAdapter, filter_mut::FilterMut, filter_ref::FilterRefAdapter, range::RangeAdapter, update::Update},
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    protocol::count::Count,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::remapping::ResponseRemapping,
    protocol::{filterable::Filterable, value::Value, return_values::ReturnValue, remapping::Remapping},
};
use sonic_rs::{Deserialize, Serialize};

#[handler]
pub fn search(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct searchData {
        vec: Vec<f64>,
        k: i32,
    }

    let data: searchData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let tr = G::new(Arc::clone(&db), &txn);
    let tr = G::new(Arc::clone(&db), &txn);
    let tr = tr.search_v::<fn(&HVector) -> bool>(&data.vec, data.k as usize, None);
    let res = tr.collect_to::<Vec<_>>();

    return_vals.insert("res".to_string(), ReturnValue::from_traversal_value_array_with_mixin(res, remapping_vals.borrow_mut()));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn kdkhn(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct kdkhnData {
        vec: Vec<Vec<f64>>,
    }

    let data: kdkhnData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    for vec in data.vec {
        let tr = G::new_mut(Arc::clone(&db), &mut txn);
        let tr = tr.insert_v::<fn(&HVector) -> bool>(&vec);
    }
    return_vals.insert("message".to_string(), ReturnValue::from("Success"));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    txn.commit()?;
    Ok(())
}

