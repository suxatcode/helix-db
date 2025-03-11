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

#[handler]
pub fn find_influential_users(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["User"]);
    tr.for_each_node(&txn, |item, txn| {
        let name_remapping = Remapping::new(true, Some("Name".to_string()), None);
        remapping_vals.borrow_mut().insert(
            item.id.clone(),
            ResponseRemapping::new(HashMap::from([("Name".to_string(), name_remapping)]), false),
        );
        Ok(())
    });
    let users = tr.finish()?;

    return_vals.insert(
        "users".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(users, remapping_vals.borrow_mut()),
    );
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

// Node Schema: User
struct User {
    Id: i32,
    Name: String,
    FollowersCount: i32,
    Verified: bool,
}

// Node Schema: Post
struct Post {
    Id: i32,
    Content: String,
    Author: i32,
    Timestamp: i32,
}
