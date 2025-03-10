use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::protocol::remapping::ResponseRemapping;
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
    let mut remapping_vals: HashMap<String, HashMap<String, Remapping>> = HashMap::new();
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["User"]);

    tr.for_each_node(&txn, |usr, txn| {
        let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(usr.clone()));
        let posts_remapping = Remapping::new(
            false,
            Some(posts),
            Some(ReturnValue::from_traversal_value_array_with_mixin(
                posts,
                remapping_vals.borrow_mut(),
            )),
        );
        remmaping_vals.borrow_mut().insert(
            usr.id.clone(),
            ResponseRemapping::new(
                HashMap::from([("posts".to_string(), posts_remapping)]),
                false,
            ),
        );
        Ok(())
    });

    let users = tr.finish()?;

    return_vals.insert("users".to_string(), ReturnValue::TraversalValues(users));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn get_posts(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut remapping_vals: HashMap<String, HashMap<String, Remapping>> = HashMap::new();
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["Post"]);
    let posts = tr.finish()?;

    return_vals.insert("posts".to_string(), ReturnValue::TraversalValues(posts));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn add_post(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut remapping_vals: HashMap<String, HashMap<String, Remapping>> = HashMap::new();
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(&mut txn, "Post", props!{ "Author".to_string() => 1, "Timestamp".to_string() => 1000000000, "Content".to_string() => "Hello, world!" }, None);
    let post = tr.finish()?;

    txn.commit()?;
    return_vals.insert("post".to_string(), ReturnValue::TraversalValues(post));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn add_posts(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut remapping_vals: HashMap<String, HashMap<String, Remapping>> = HashMap::new();
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(&mut txn, "Post", props!{ "Author".to_string() => 1, "Timestamp".to_string() => 1000000000, "Content".to_string() => "Hello, world!" }, None);
    let posts = tr.finish()?;

    txn.commit()?;
    return_vals.insert("posts".to_string(), ReturnValue::TraversalValues(posts));
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
