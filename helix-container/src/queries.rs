
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::{field_remapping, identifier_remapping, traversal_remapping, exclude_field};
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::{AddEAdapter, EdgeType},
            add_n::AddNAdapter,
            e::EAdapter,
            e_from_id::EFromIdAdapter,
            e_from_type::EFromTypeAdapter,
            n::NAdapter,
            n_from_id::NFromIdAdapter,
            n_from_type::NFromTypeAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, drop::DropAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, range::RangeAdapter, update::UpdateAdapter,
            map::MapAdapter, paths::ShortestPathAdapter, props::PropsAdapter, drop::Drop,
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
        filterable::Filterable, remapping::Remapping, return_values::ReturnValue, value::Value, id::ID,
    },
};
use sonic_rs::{Deserialize, Serialize};
    
pub struct Record {
    pub data: String,
}



#[derive(Serialize, Deserialize)]
pub struct scan_recordsInput {

pub limit: i32,
pub offset: i32
}
#[handler]
pub fn scan_records (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: scan_recordsInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    let records = G::new(Arc::clone(&db), &txn)
.n_from_type("Record").collect_to::<Vec<_>>();
        return_vals.insert("records".to_string(), ReturnValue::from_traversal_value_array_with_mixin(records, remapping_vals));

    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct read_recordInput {

pub id: ID
}
#[handler]
pub fn read_record (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: read_recordInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    let record = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.id).collect_to::<Vec<_>>();
        return_vals.insert("record".to_string(), ReturnValue::from_traversal_value_array_with_mixin(record, remapping_vals));

    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct count_recordsInput {


}
#[handler]
pub fn count_records (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: count_recordsInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    let count = G::new(Arc::clone(&db), &txn)
.n_from_type("Record")

.count();
        return_vals.insert(count.to_string(), ReturnValue::from(Value::from(count)));

    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct create_recordInput {

pub data: String
}
#[handler]
pub fn create_record (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: create_recordInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    let record = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("Record", props! { "data" => data.data }, None).collect_to::<Vec<_>>();
        return_vals.insert("record".to_string(), ReturnValue::from_traversal_value_array_with_mixin(record, remapping_vals));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct delete_recordInput {

pub id: ID
}
#[handler]
pub fn delete_record (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: delete_recordInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    Drop::<Vec<_>>::drop_traversal(
                G::new(Arc::clone(&db), &txn)
.n_from_id(&data.id).collect::<Vec<_>>(),
                Arc::clone(&db),
                &mut txn,
            )?;
        return_vals.insert("NONE".to_string(), ReturnValue::from(Value::from("NONE")));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct update_recordInput {

pub id: ID,
pub data: String
}
#[handler]
pub fn update_record (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: update_recordInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    let record = {let update_tr = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.id)
    .collect_to::<Vec<_>>();G::new_mut_from(Arc::clone(&db), &mut txn, update_tr)
    .update(props! { "data" => data.data })
    .collect_to::<Vec<_>>()};
        return_vals.insert("record".to_string(), ReturnValue::from_traversal_value_array_with_mixin(record, remapping_vals));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
