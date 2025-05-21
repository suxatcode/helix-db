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
    pub node_id: ID,
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
        .n_from_id(&data.node_id)
        .out("Knows")
        .collect_to::<Vec<_>>();
    let old_users = G::new_from(Arc::clone(&db), &txn, user_nodes.clone())
        .filter_ref(|val, txn| {
            if let Ok(val) = val {
                Ok(val.check_property("age").map_or(false, |v| *v == 60))
            } else {
                Ok(false)
            }
        })
        .collect_to::<Vec<_>>();
    return_vals.insert("old_users".to_string(), ReturnValue::from_traversal_value_array_with_mixin(G::new_from(Arc::clone(&db), &txn, old_users.clone())

.map_traversal(|u, txn| { identifier_remapping!(remapping_vals, u.clone(), "age" => "age")?;
traversal_remapping!(remapping_vals, u.clone(), "username" => G::new_from(Arc::clone(&db), txn, vec![u.clone()])

.check_property("name")
    .collect_to::<Vec<_>>())?;
 Ok(u) })
    .collect_to::<Vec<_>>(), remapping_vals));

    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct vecsData {
    pub id: ID,
    pub name: String,
}
#[derive(Serialize, Deserialize)]
pub struct nodesData {
    pub id: ID,
    pub vecs: Vec<vecsData>,
}
#[derive(Serialize, Deserialize)]
pub struct get_user_with_friendsInput {
    pub nodes: Vec<nodesData>,
    pub user_id: ID,
}
#[handler]
pub fn get_user_with_friends(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let data: get_user_with_friendsInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: HashMap<u128, ResponseRemapping> = HashMap::new();
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    for data in data.nodes {
        let user_nodes = G::new(Arc::clone(&db), &txn)
            .n_from_id(&data.id)
            .out("Knows")
            .collect_to::<Vec<_>>();
        for data in data.vecs {
            let user_node = G::new(Arc::clone(&db), &txn)
                .n_from_id(&data.id)
                .out("Knows")
                .collect_to::<Vec<_>>();
        }
    }

    return_vals.insert("success".to_string(), ReturnValue::from("success"));

    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
