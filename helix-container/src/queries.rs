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
    tr.for_each_node(&txn, |usr, txn| {
        let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(usr.clone()));
        tr.out(&txn, "Posts");
        tr.for_each_node(&txn, |item, txn| {
            let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(item.clone()));
            let creator_id = tr.finish()?.get_id()?;
            let creator_id_remapping = Remapping::new(
                false,
                Some("creatorId".to_string()),
                Some(ReturnValue::from(creator_id)),
            );
            remapping_vals.borrow_mut().insert(
                item.id.clone(),
                ResponseRemapping::new(
                    HashMap::from([("creatorId".to_string(), creator_id_remapping)]),
                    true,
                ),
            );
            Ok(())
        });
        let posts = tr.finish()?;
        let posts_remapping = Remapping::new(
            false,
            Some("posts".to_string()),
            Some(ReturnValue::from_traversal_value_array_with_mixin(
                posts,
                remapping_vals.borrow_mut(),
            )),
        );
        let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(usr.clone()));
        tr.for_each_node(&txn, |item, txn| {
            let name = item.check_property("Name");
            let name_remapping = Remapping::new(
                false,
                None,
                Some(match name {
                    Some(value) => ReturnValue::from(value.clone()),
                    None => {
                        return Err(GraphError::ConversionError(
                            "Property not found on name".to_string(),
                        ))
                    }
                }),
            );
            remapping_vals.borrow_mut().insert(
                item.id.clone(),
                ResponseRemapping::new(
                    HashMap::from([("Name".to_string(), name_remapping)]),
                    false,
                ),
            );
            Ok(())
        });
        let user_name = tr.finish()?;
        let user_name_remapping = Remapping::new(
            false,
            Some("userName".to_string()),
            Some(ReturnValue::from_traversal_value_array_with_mixin(
                user_name,
                remapping_vals.borrow_mut(),
            )),
        );
        remapping_vals.borrow_mut().insert(
            usr.id.clone(),
            ResponseRemapping::new(
                HashMap::from([
                    ("posts".to_string(), posts_remapping),
                    ("userName".to_string(), user_name_remapping),
                ]),
                true,
            ),
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


#[handler]
pub fn add_posts(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut remapping_vals: HashMap<String, ResponseRemapping> = HashMap::new();
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(&mut txn, "Post", props!{ "Author".to_string() => 1, "Timestamp".to_string() => 1000000000, "Content".to_string() => "Hello, world!" }, None);
    let post = tr.finish()?;
    let post_id = post.get_id()?;

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_types(&txn, &["User"]);
    let users = tr.finish()?;

    if let TraversalValue::NodeArray(users) = users {
        for usr in users {
            let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
            tr.add_e(
                &mut txn,
                "Posts",
                &usr.id,
                &post_id,
                props! { "Timestamp".to_string() => 1000000000 },
            );
        }
    }

    txn.commit()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn add_user(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);
    let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(&mut txn, "User", props!{ "Name".to_string() => "John Doe".to_string(), "FollowersCount".to_string() => 100, "Verified".to_string() => true }, None);
    let users = tr.finish()?;

    txn.commit()?;
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
