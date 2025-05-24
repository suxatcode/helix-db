use get_routes::handler;
use heed3::RoTxn;
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{exclude_field, field_remapping, identifier_remapping, traversal_remapping};
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
            dedup::DedupAdapter, drop::Drop, drop::DropAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, map::MapAdapter, paths::ShortestPathAdapter,
            props::PropsAdapter, range::RangeAdapter, update::UpdateAdapter,
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

pub struct File7 {
    pub name: String,
    pub age: i32,
}

pub struct EdgeFile7 {
    pub from: File7,
    pub to: File7,
}

pub struct File7Vec {
    pub content: String,
}

#[derive(Serialize, Deserialize)]
pub struct file7Input {
    pub vec: Vec<f64>,
}
#[handler]
pub fn file7(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: file7Input = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    let vecs = G::new(Arc::clone(&db), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool>(&data.vec, 10, None)
        .collect_to::<Vec<_>>();
    return_vals.insert("hello".to_string(), ReturnValue::from(Value::from("hello")));

    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
