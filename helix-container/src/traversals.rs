use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use get_routes::handler;
use helix_engine::graph_core::traversal::TraversalBuilder;
use helix_engine::graph_core::traversal_steps::{
    RSourceTraversalSteps, RTraversalBuilderMethods, RTraversalSteps, TraversalMethods,
    TraversalSearchMethods, WSourceTraversalSteps, WTraversalBuilderMethods, WTraversalSteps,
};
use helix_engine::types::GraphError;
use helix_engine::{node_matches, props};
use helix_gateway::router::router::HandlerInput;
use protocol::count::Count;
use protocol::response::Response;
use protocol::traversal_value::TraversalValue;
use protocol::{filterable::Filterable, value::Value, ReturnValue};
use sonic_rs::{Deserialize, Serialize};

#[derive(Deserialize)]
struct SubGraphData {
    user_id: String,
    username: String,
    limit: usize,
}

#[handler]
pub fn get_user_subgraph(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let start = Instant::now();
    let data: SubGraphData = sonic_rs::from_slice(&input.request.body).unwrap();
    let mut return_vals = HashMap::with_capacity(4);
    let db = input.graph.storage.clone();
    let txn = db.env.read_txn().unwrap();
    println!("Setup took: {}ms", start.elapsed().as_millis());

    let limit = match data.limit {
        0 => 400,
        _ => data.limit,
    };

    let start_user = std::time::Instant::now();
    let mut user = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    match data.username.len() {
        0 => {
            user.v(&txn).range(0, 1);
        }
        _ => {
            // user.v().filter_nodes(node_matches!("username", data.username));
            user.v_from_secondary_index(&txn, "username", &Value::String(data.username));
        }
    }
    let user = user.result(txn)?;
    println!("User lookup took: {}ms", start_user.elapsed().as_millis());

    // get 350 out nodes that are mutuals
    let start_mutuals = std::time::Instant::now();
    let txn = db.env.read_txn().unwrap();
    let mut mutuals = TraversalBuilder::new(Arc::clone(&db), user.clone());
    mutuals.mutual(&txn, "follows").range(0, limit);
    let mutuals = mutuals.result(txn)?;
    println!(
        "Mutuals traversal took: {}ms",
        start_mutuals.elapsed().as_millis()
    );

    return_vals.insert("user".to_string(), ReturnValue::TraversalValues(user));
    return_vals.insert("mutuals".to_string(), ReturnValue::TraversalValues(mutuals));
    let start_serialize = std::time::Instant::now();
    response.body = match sonic_rs::to_vec(&return_vals) {
        Ok(body) => body,
        Err(e) => {
            return Err(GraphError::ConversionError(e.to_string()));
        }
    };
    println!(
        "JSON serialization took: {}ms",
        start_serialize.elapsed().as_millis()
    );
    Ok(())
}

#[derive(Deserialize)]
struct ShortestPathData {
    user_id: String,
    to_id: String,
}
#[handler]
pub fn get_shortest_path_to_user(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(2);

    let data: ShortestPathData = sonic_rs::from_slice(&input.request.body).unwrap();
    let now = Instant::now();
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.env.read_txn().unwrap();
    let mut user = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

    user.v_from_id(&txn, data.user_id.as_str())
        .shortest_path_to(&txn, data.to_id.as_str());

    let result = user.result(txn)?;
    let end = now.elapsed();
    return_vals.insert(
        "time".to_string(),
        ReturnValue::Count(Count::new(end.as_millis() as usize)),
    );
    return_vals.insert(
        "shortest_path".to_string(),
        ReturnValue::TraversalValues(result),
    );
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Deserialize)]
struct AddUser {
    x_id: String,
    username: String,
    url: String,
    location: String,
    verified: bool,
    followers_count: i32,
    following_count: i32,
    post_count: i32,
    joined_date: String,
    profile_image_url: String,
    profile_banner_url: String,
    graph_image_url: String,
    created_at: String,
    updated_at: String,
    following_ids: Vec<String>,
}

#[handler]
pub fn add_user(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(2);
    let data: AddUser = sonic_rs::from_slice(&input.request.body).unwrap();

    let now = Instant::now();
    let db = Arc::clone(&input.graph.storage);

    let mut txn = db.env.write_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_secondary_index(&txn, "username", &Value::String(data.username.clone()));

    let user = tr.current_step;
    match user {
        TraversalValue::NodeArray(nodes) => {
            if nodes.len() > 1 {
                // if there are more than one user with the same x_id
                return Err(GraphError::from(
                    "Multiple users with the same username".to_string(),
                ));
            } else if nodes.len() == 1 {
                match nodes.first() {
                    Some(node) => match node.check_property("is_enabled").unwrap() {
                        Value::Boolean(b) => match b {
                            true => {
                                return Err(GraphError::from("User already exists".to_string()))
                            }
                            false => {
                                let mut tr =
                                    TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
                                tr.v_from_id(&txn, &node.id);
                                tr.update_props(
                                    &mut txn,
                                    props! {
                                        "username" => data.username,
                                        "url" => data.url,
                                        "location" => data.location,
                                        "verified" => data.verified,
                                        "followers_count" => data.followers_count,
                                        "following_count" => data.following_count,
                                        "post_count" => data.post_count,
                                        "joined_date" => data.joined_date,
                                        "profile_image_url" => data.profile_image_url,
                                        "profile_banner_url" => data.profile_banner_url,
                                        "graph_image_url" => data.graph_image_url,
                                        "created_at" => data.created_at,
                                        "updated_at" => data.updated_at,
                                        "is_enabled" => true,
                                    },
                                );
                                let updated_user = tr.current_step;
                                return_vals.insert(
                                    "updated_user".to_string(),
                                    ReturnValue::TraversalValues(updated_user),
                                );
                            }
                        },
                        _ => return Err(GraphError::from("Invalid node".to_string())),
                    },
                    None => return Err(GraphError::from("Invalid node".to_string())),
                }
            }
        }
        TraversalValue::Empty => {
            let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
            tr.add_v(
                &mut txn,
                "user",
                props! {
                    "x_id" => data.x_id,
                    "username" => data.username,
                    "url" => data.url,
                    "location" => data.location,
                    "verified" => data.verified,
                    "followers_count" => data.followers_count,
                    "following_count" => data.following_count,
                    "post_count" => data.post_count,
                    "joined_date" => data.joined_date,
                    "profile_image_url" => data.profile_image_url,
                    "profile_banner_url" => data.profile_banner_url,
                    "graph_image_url" => data.graph_image_url,
                    "created_at" => data.created_at,
                    "updated_at" => data.updated_at,
                    "is_enabled" => true,
                },
                Some(&["username".to_string(), "x_id".to_string()]),
            );
            let added_user = tr.current_step;
            let user = match added_user.clone() {
                TraversalValue::NodeArray(nodes) => nodes.first().unwrap().clone(),
                _ => return Err(GraphError::from("Invalid node".to_string())),
            };
            // let following_ids: HashSet<String> = HashSet::from_iter(data.following_ids);
            // The issue is in the following_ids.iter().for_each() closure
            // We need to handle the Result types properly and not return them within the for_each

            data.following_ids.iter().for_each(|id: &String| {
                let mut following = TraversalBuilder::new(Arc::clone(&db), added_user.clone());
                following.v_from_secondary_index(&txn, "x_id", &Value::String(id.to_string()));

                match following.current_step {
                    TraversalValue::NodeArray(nodes) => {
                        if nodes.len() == 1 {
                            let node = nodes.first().unwrap();
                            let mut edge = TraversalBuilder::new(
                                Arc::clone(&input.graph.storage),
                                TraversalValue::Empty,
                            );
                            edge.add_e(&mut txn, "follows", &user.id, &node.id, props! {});
                            if !matches!(edge.current_step, TraversalValue::EdgeArray(_)) {
                                println!("Failed to create edge");
                            }
                        }
                    }
                    TraversalValue::Empty => {
                        println!("User with id {} not found", id);
                        let mut new_node = TraversalBuilder::new(
                            Arc::clone(&input.graph.storage),
                            TraversalValue::Empty,
                        );
                        new_node.add_v(
                            &mut txn,
                            "user",
                            props! {
                                "x_id" => id.to_string(),
                                "created_at" => "2021-01-01".to_string(),
                                "updated_at" => "2021-01-01".to_string(),
                                "is_enabled" => false,
                            },
                            Some(&["username".to_string(), "x_id".to_string()]),
                        );
                        if let TraversalValue::NodeArray(nodes) = new_node.current_step {
                            let node = nodes.first().unwrap();
                            let mut edge = TraversalBuilder::new(
                                Arc::clone(&input.graph.storage),
                                TraversalValue::Empty,
                            );
                            edge.add_e(&mut txn, "follows", &user.id, &node.id, props! {});
                            if !matches!(edge.current_step, TraversalValue::EdgeArray(_)) {
                                println!("Failed to create edge");
                            }
                        }
                    },
                    _ => {
                        println!("Invalid node");
                        panic!("Invalid node");
                    }
                }
            });

            return_vals.insert(
                "new_user".to_string(),
                ReturnValue::TraversalValues(added_user),
            );
        }
        _ => return Err(GraphError::from("Invalid node".to_string())),
    }
    txn.commit()?;

    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

// do type checking based on type of transaction do matches based on that
// allow all reads to be done on writes
// allow writes only on reads
