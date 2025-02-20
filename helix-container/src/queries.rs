use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::{
    helix_engine::graph_core::traversal::TraversalBuilder,
    helix_engine::graph_core::traversal_steps::{
        RSourceTraversalSteps, RTraversalBuilderMethods, RTraversalSteps, TraversalMethods,
        TraversalSearchMethods, WSourceTraversalSteps, WTraversalBuilderMethods, WTraversalSteps,
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::{filterable::Filterable, return_values::ReturnValue, value::Value},
};
use sonic_rs::{Deserialize, Serialize};

#[handler]
pub fn find_influential_users(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["User"]);
    tr.filter_nodes(&txn, |node| {
        Ok({
            let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));
            tr.out(&txn, "Follows");
            tr.count();
            let count = tr.finish()?.as_count().unwrap();
            count > 100
        })
    });
    tr.filter_nodes(&txn, |node| {
        Ok({
            let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));
            tr.in_(&txn, "Follows");
            tr.count();
            let count = tr.finish()?.as_count().unwrap();
            count > 1000
        })
    });
    let users = tr.finish()?;

    return_vals.insert("users".to_string(), ReturnValue::TraversalValues(users));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn find_complex_users(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["User"]);
    tr.filter_nodes(&txn, |node| {
        Ok((node
            .check_property("verified")
            .map_or(false, |v| matches!(v, Value::Boolean(val) if *val == true))
            || node
                .check_property("followers_count")
                .map_or(false, |v| matches!(v, Value::Integer(val) if *val > 5000)))
            && {
                let mut tr =
                    TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));
                tr.out(&txn, "Authored");
                tr.count();
                let count = tr.finish()?.as_count().unwrap();
                count > 10
            })
    });
    let users = tr.finish()?;

    return_vals.insert("users".to_string(), ReturnValue::TraversalValues(users));
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
