<<<<<<< HEAD


use heed3::RoTxn;
use get_routes::handler;
use heed3::RoTxn;
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{exclude_field, field_remapping, identifier_remapping, traversal_remapping};
use helixdb::{
    helix_engine::graph_core::ops::{
        bm25::search_bm25::SearchBM25Adapter,
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::{AddEAdapter, EdgeType},
            add_n::AddNAdapter,
            e_from_id::EFromIdAdapter,
            e_from_type::EFromTypeAdapter,
            n_from_id::NFromIdAdapter,
            n_from_index::NFromIndexAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, range::RangeAdapter, update::UpdateAdapter,
            map::MapAdapter, paths::ShortestPathAdapter, props::PropsAdapter, drop::Drop,
=======
            n_from_index::NFromIndexAdapter,
            n_from_type::NFromTypeAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, drop::Drop, filter_mut::FilterMut, filter_ref::FilterRefAdapter,
            map::MapAdapter, paths::ShortestPathAdapter, props::PropsAdapter, range::RangeAdapter,
            update::UpdateAdapter,
>>>>>>> main
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
        filterable::Filterable, id::ID, remapping::Remapping, return_values::ReturnValue,
        value::Value,
    },
};
use sonic_rs::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use std::cell::RefCell;
use chrono::{DateTime, Utc};



pub struct Embedding {
    pub vec: Vec<f64>,
}

#[derive(Serialize, Deserialize)]
pub struct hnswsearchInput {

pub query: Vec<f64>,
pub k: i32
}
#[handler]
pub fn hnswsearch (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: hnswsearchInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph);
let txn = db.graph_env.read_txn().unwrap();
    let res = G::new(Arc::clone(&db), &txn)
.search_v::<fn(&HVector, &RoTxn) -> bool>(&data.query, data.k as usize, None).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("res".to_string(), ReturnValue::from_traversal_value_array_with_mixin(res.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct hnswinsertInput {

pub vector: Vec<f64>
}
#[handler]
pub fn hnswinsert (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: hnswinsertInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph);
let mut txn = db.graph_env.write_txn().unwrap();
    let res = G::new_mut(Arc::clone(&db), &mut txn)
.insert_v::<fn(&HVector, &RoTxn) -> bool>(&data.vector, "Embedding", None).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
println!("res: {:?}", &res);
        return_vals.insert("res".to_string(), ReturnValue::from_traversal_value_array_with_mixin(res.clone(), remapping_vals.borrow_mut()));
=======
        filterable::Filterable, id::ID, remapping::Remapping, return_values::ReturnValue,
        value::Value,
    },
};
use sonic_rs::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

pub struct File8 {
    pub name: String,
    pub age: i32,
}

pub struct EdgeFile8 {
    pub from: File8,
    pub to: File8,
}

pub struct File8Vec {
    pub content: String,
}

#[handler]
pub fn file8(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let res = G::new(Arc::clone(&db), &txn)
        .search_bm25("File8Vec", "John", 10)
        .collect_to::<Vec<_>>();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    return_vals.insert(
        "res".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(
            res.clone(),
            remapping_vals.borrow_mut(),
        ),
    );
>>>>>>> main

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
