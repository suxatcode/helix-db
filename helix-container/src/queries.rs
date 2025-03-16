use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::{
    helix_engine::graph_core::traversal::TraversalBuilder,
    helix_engine::graph_core::traversal_steps::{
        SourceTraversalSteps, TraversalBuilderMethods, TraversalMethods, TraversalSearchMethods,
        TraversalSteps,
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

// Node Schema: User
#[derive(Serialize, Deserialize)]
struct User {
    name: String,
    followers_count: i32,
    verified: bool,
}

// Node Schema: Post
#[derive(Serialize, Deserialize)]
struct Post {
    content: String,
    author: i32,
    timestamp: i32,
}

// Edge Schema: Authored
#[derive(Serialize, Deserialize)]
struct Authored {
    timestamp: i32,
}

// Edge Schema: Follows
#[derive(Serialize, Deserialize)]
struct Follows {
    timestamp: i32,
}

#[handler]
pub fn tr_with_array_param(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct trWithArrayParamData {
        users: Vec<User>,
        meal_i_d: String,
    }

    let data: trWithArrayParamData = sonic_rs::from_slice(&input.request.body).unwrap();

    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    let e = tr.finish()?;

    return_vals.insert("message".to_string(), ReturnValue::from("SUCCESS"));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}
