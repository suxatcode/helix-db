use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
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
            dedup::DedupAdapter, drop::DropAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, map::MapAdapter, range::RangeAdapter,
            update::UpdateAdapter,
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

pub struct User {
    pub name: String,
    pub age: i32,
}

pub struct Knows {
    pub from: User,
    pub to: User,
    pub since: i32,
}

#[derive(Serialize, Deserialize)]
pub struct get_userInput {
    pub name: String,
}
#[handler]
pub fn get_user(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: get_userInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    let user_nodes = G::new(Arc::clone(&db), &txn)
        .n_from_type("User")
        .filter_ref(|val, txn| {
            if let Ok(val) = val {
                Ok(G::new_from(Arc::clone(&db), txn, vec![val.clone()])
                    .out("Knows")
                    .count()
                    .gt(&0))
            } else {
                Ok(false)
            }
        })
        .collect_to::<Vec<_>>();
    return_vals.insert(
        "user_nodes".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(user_nodes, remapping_vals),
    );

    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
