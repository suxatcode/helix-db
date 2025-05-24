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

#[derive(Serialize, Deserialize)]
pub struct file8Input {
    pub vec: Vec<f64>,
}
#[handler]
pub fn file8(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: file8Input = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    let new_vec = G::new_mut(Arc::clone(&db), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            &data.vec,
            "File8Vec",
            Some(props! { "content" => "hello" }),
        )
        .collect_to::<Vec<_>>();
    return_vals.insert(
        "new_vec".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(new_vec, remapping_vals),
    );

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
