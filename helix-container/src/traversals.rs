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
use serde::de;
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
    let txn = db.graph_env.read_txn().unwrap();
    println!("Setup took: {}ms", start.elapsed().as_millis());

    let limit = match data.limit {
        0 => 400,
        _ => data.limit,
    };

    let start_user = std::time::Instant::now();
    let mut user = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    match data.username.len() {
        0 => {
            let node = db.get_random_node(&txn)?;
            user = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node));
        }
        _ => {
            // user.v().filter_nodes(node_matches!("username", data.username));
            user.v_from_secondary_index(&txn, "username", &Value::String(data.username));
        }
    }
    let user = user.result(txn)?;
    println!("User lookup took: {}ms", start_user.elapsed().as_millis());
    println!("User: {:?}", user);
    // get 350 out nodes that are mutuals
    let start_mutuals = std::time::Instant::now();
    let txn = db.graph_env.read_txn().unwrap();
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
    let mut txn = db.graph_env.read_txn().unwrap();
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

    let mut txn = db.graph_env.write_txn().unwrap();
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

            data.following_ids.iter().for_each(|id: &String| {
                let mut following = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
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
                    }
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

#[handler]
pub fn count_nodes(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v(&txn).count();
    let result = match tr.result(txn) {
        Ok(TraversalValue::Count(c)) => c,
        _ => Count::new(0),
    };
    return_vals.insert("count".to_string(), ReturnValue::Count(result));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn count_edges(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.e(&txn).count();
    let result = match tr.result(txn) {
        Ok(TraversalValue::Count(c)) => c,
        _ => Count::new(0),
    };
    return_vals.insert("count".to_string(), ReturnValue::Count(result));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct User {
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

#[derive(Serialize, Deserialize)]
struct Data<'a> {
    path: Option<&'a str>,
}
#[handler]
pub fn upload_all(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let d = sonic_rs::from_slice::<Data>(&input.request.body).unwrap();
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);
    let db = Arc::clone(&input.graph.storage);

    // read json from from /home/ec2-user/convert/output_users.json
    let json_str =
        std::fs::read_to_string(d.path.unwrap_or("/home/ec2-user/convert/output_users.json"))
            .unwrap();
    let users: Vec<User> = sonic_rs::from_str(&json_str).unwrap();

    //for each user run add user
    let mut txn = db.graph_env.write_txn().unwrap();
    for data in users {
        let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
        tr.v_from_secondary_index(&txn, "x_id", &Value::String(data.x_id.clone()));

        let user = tr.current_step;
        println!("User: {:?}", user);
        match user {
            TraversalValue::NodeArray(nodes) => {
                if nodes.len() > 1 {
                    // if there are more than one user with the same x_id
                    return Err(GraphError::from(
                        "Multiple users with the same username".to_string(),
                    ));
                } else if nodes.len() == 1 {
                    println!("Updating user");
                    match nodes.first() {
                        Some(node) => match node.check_property("is_enabled").unwrap() {
                            Value::Boolean(b) => match b {
                                true => {
                                    return Err(GraphError::from("User already exists".to_string()))
                                }
                                false => {
                                    let mut tr = TraversalBuilder::new(
                                        Arc::clone(&db),
                                        TraversalValue::Empty,
                                    );
                                    tr.v_from_id(&txn, &node.id);
                                    tr.update_props(
                                        &mut txn,
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
                                    );
                                    let updated_user = tr.finish()?;
                                    let user = match updated_user.clone() {
                                        TraversalValue::NodeArray(nodes) => {
                                            nodes.first().unwrap().clone()
                                        }
                                        _ => {
                                            return Err(GraphError::from(
                                                "Invalid node".to_string(),
                                            ))
                                        }
                                    };

                                    data.following_ids.iter().for_each(|x_id: &String| {
                                        let mut following = TraversalBuilder::new(
                                            Arc::clone(&db),
                                            TraversalValue::Empty,
                                        );
                                        following.v_from_secondary_index(
                                            &txn,
                                            "x_id",
                                            &Value::String(x_id.to_string()),
                                        );

                                        match following.current_step {
                                            TraversalValue::NodeArray(nodes) => {
                                                if nodes.len() == 1 {
                                                    let node = nodes.first().unwrap();
                                                    let mut edge = TraversalBuilder::new(
                                                        Arc::clone(&input.graph.storage),
                                                        TraversalValue::Empty,
                                                    );
                                                    edge.add_e(
                                                        &mut txn,
                                                        "follows",
                                                        &user.id,
                                                        &node.id,
                                                        props! {},
                                                    );
                                                    if !matches!(
                                                        edge.current_step,
                                                        TraversalValue::EdgeArray(_)
                                                    ) {
                                                        println!("Failed to create edge");
                                                    }
                                                }
                                            }
                                            TraversalValue::Empty => {
                                                println!("User with id {} not found", x_id);
                                                let mut new_node = TraversalBuilder::new(
                                                    Arc::clone(&input.graph.storage),
                                                    TraversalValue::Empty,
                                                );
                                                new_node.add_v(
                                                    &mut txn,
                                                    "user",
                                                    props! {
                                                        "x_id" => x_id.to_string(),
                                                        "created_at" => "2021-01-01".to_string(),
                                                        "updated_at" => "2021-01-01".to_string(),
                                                        "is_enabled" => false,
                                                    },
                                                    Some(&["x_id".to_string()]),
                                                );
                                                if let TraversalValue::NodeArray(nodes) =
                                                    new_node.finish().unwrap()
                                                {
                                                    let node = nodes.first().unwrap();
                                                    let mut edge = TraversalBuilder::new(
                                                        Arc::clone(&input.graph.storage),
                                                        TraversalValue::Empty,
                                                    );
                                                    edge.add_e(
                                                        &mut txn,
                                                        "follows",
                                                        &user.id,
                                                        &node.id,
                                                        props! {},
                                                    );
                                                    if !matches!(
                                                        edge.current_step,
                                                        TraversalValue::EdgeArray(_)
                                                    ) {
                                                        println!("Failed to create edge");
                                                    }
                                                }
                                            }
                                            _ => {
                                                println!("Invalid node");
                                                panic!("Invalid node");
                                            }
                                        }
                                    });

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
                println!("Adding user");
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

                data.following_ids.iter().for_each(|x_id: &String| {
                    let mut following =
                        TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
                    following.v_from_secondary_index(
                        &txn,
                        "x_id",
                        &Value::String(x_id.to_string()),
                    );

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
                            println!("User with id {} not found", x_id);
                            let mut new_node = TraversalBuilder::new(
                                Arc::clone(&input.graph.storage),
                                TraversalValue::Empty,
                            );
                            new_node.add_v(
                                &mut txn,
                                "user",
                                props! {
                                    "x_id" => x_id.to_string(),
                                    "created_at" => "2021-01-01".to_string(),
                                    "updated_at" => "2021-01-01".to_string(),
                                    "is_enabled" => false,
                                },
                                Some(&["x_id".to_string()]),
                            );
                            if let TraversalValue::NodeArray(nodes) = new_node.finish().unwrap() {
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
    }

    txn.commit()?;
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn batch_create(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    println!(" ");
    println!(" ");
    println!(" ");
    println!(" ");

    println!("  Fetching 100_000 nodes");
    let now = Instant::now();
    let mut txn = db.graph_env.write_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v(&txn);
    let n = tr.result(txn)?;
    let nodes = match n {
        TraversalValue::NodeArray(nodes) => nodes,
        _ => return Err(GraphError::from("Invalid node".to_string())),
    };
    let end = now.elapsed();
    println!("  Fetching 100_000 nodes took: {}ms", end.as_millis());
    println!(" ");
    println!(" ");
    println!(" ");
    println!(" ");
    response.body = sonic_rs::to_vec(&end.as_millis()).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct RSUser {
    cognito_id: String,
    username: String,
}

#[handler]
pub fn create_user(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let data = sonic_rs::from_slice::<RSUser>(&input.request.body)?;
    let mut txn = db.graph_env.write_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(
        &mut txn,
        "user",
        props! {
            "username" => data.username,
            "cognito_id" => data.cognito_id,
            "created_at" => chrono::Utc::now().to_rfc3339(),
        },
        Some(&["cognito_id".to_string()]),
    );
    let user = tr.result(txn)?;
    response.body = sonic_rs::to_vec(&user).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Chat {
    cognito_id: String,
    chat_name: String,
}

#[handler]
pub fn create_chat(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let data = sonic_rs::from_slice::<Chat>(&input.request.body).map_err(|e|GraphError::from(format!("CREATE CHAT ERROR {:?}",e)))?;
    let mut txn = db.graph_env.write_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(
        &mut txn,
        "chat",
        props! {
            "chat_name" => data.chat_name,
            "created_at" => chrono::Utc::now().to_rfc3339(),
        },
        None,
    );
    let chat_id = match tr.finish()? {
        TraversalValue::NodeArray(nodes) => nodes.first().unwrap().id.clone(),
        _ => return Err(GraphError::from("Error creating chat".to_string())),
    };

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_secondary_index(&txn, "cognito_id", &Value::String(data.cognito_id));
    tr.add_e_to(
        &mut txn,
        "created_chat",
        &chat_id,
        props! { "created_at" => chrono::Utc::now().to_rfc3339() },
    );
    tr.result(txn)?;

    response.body = sonic_rs::to_vec(&chat_id).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Message {
    cognito_id: String,
    chat_id: String,
    chat_name: String,
    message: String,
    message_type: String,
}

#[handler]
pub fn add_message(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let data = sonic_rs::from_slice::<Message>(&input.request.body).map_err(|e|GraphError::from(format!("ADD ERROR {:?}",e)))?;
    let mut txn = db.graph_env.write_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.add_v(
        &mut txn,
        "message",
        props! {
            "message" => data.message,
            "message_type" => data.message_type,
            "created_at" => chrono::Utc::now().to_rfc3339(),
        },
        None,
    );
    let message_id = match tr.current_step {
        TraversalValue::NodeArray(nodes) => nodes.first().unwrap().id.clone(),
        _ => return Err(GraphError::from("Invalid node".to_string())),
    };

    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_id(&txn, data.chat_id.as_str());
    tr.add_e_to(
        &mut txn,
        "chat_message",
        &message_id,
        props! { "created_at" => chrono::Utc::now().to_rfc3339() },
    );

    tr.result(txn)?;

    response.body = sonic_rs::to_vec(&message_id).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct ChatRequest {
    cognito_id: String,
}

#[handler]
pub fn get_chats_and_messages(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let data = sonic_rs::from_slice::<ChatRequest>(&input.request.body).map_err(|e|GraphError::from(format!("GET ERROR {:?}",e)))?;
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    println!("getting secondary index");
    tr.v_from_secondary_index(&txn, "cognito_id", &Value::String(data.cognito_id));
    println!("getting created_chat");
    tr.out(&txn, "created_chat");
    println!("getting chat_message");
    let chats = tr.finish()?;

    println!("Chats: {:?}", chats);

    #[derive(Serialize)]
    struct Message {
        message: String,
        message_type: String,
        created_at: String,
    }

    #[derive(Serialize)]
    struct ChatMessage {
        chat_id: String,
        chat_name: String,
        messages: Vec<Message>,
    }

    let mut chat_messages = HashMap::new();

    match chats {
        TraversalValue::NodeArray(chats) => {
            for chat in chats {
                let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
                tr.v_from_id(&txn, &chat.id);
                tr.out(&txn, "chat_message");
                let messages = tr.finish()?;
                let messages: Vec<Message> = match messages {
                    TraversalValue::NodeArray(messages) => messages
                        .iter()
                        .map(|message| {
                            let message = Message {
                                message: match message.check_property("message") {
                                    Some(Value::String(s)) => s.clone(),
                                    _ => "".to_string(),
                                },
                                message_type: match message.check_property("message_type") {
                                    Some(Value::String(s)) => s.clone(),
                                    _ => "".to_string(),
                                },
                                created_at: match message.check_property("created_at") {
                                    Some(Value::String(s)) => s.clone(),
                                    _ => "".to_string(),
                                },
                            };
                            message
                        })
                        .collect(),
                    _ => vec![],
                };
                chat_messages.insert(
                    chat.id.clone(),
                    ChatMessage {
                        chat_id: chat.id.clone(),
                        chat_name: {
                            match chat.check_property("chat_name") {
                                Some(Value::String(s)) => s.clone(),
                                Some(_) => "".to_string(),
                                None => "".to_string(),
                            }
                        },

                        messages,
                    },
                );
            }
        }
        _ => {}
    }

    txn.commit()?;

    response.body = sonic_rs::to_vec(&chat_messages).unwrap();
    Ok(())
}


#[handler]
pub fn drop_chats_and_messages(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();
    let data = sonic_rs::from_slice::<ChatRequest>(&input.request.body)?;
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v_from_secondary_index(&txn, "cognito_id", &Value::String(data.cognito_id));
    let chats = tr.finish()?;

    match chats {
        TraversalValue::NodeArray(chats) => {
            for chat in chats {
                let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
                tr.v_from_id(&txn, &chat.id);
                tr.out(&txn, "chat_message").drop(&mut txn);
                
                let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
                tr.v_from_id(&txn, &chat.id);
                tr.drop(&mut txn);
            }
        }
        _ => {}
    }
    
    txn.commit()?;


    response.body = sonic_rs::to_vec(&"Success").unwrap();
    Ok(())
}

#[handler]
pub fn drop_all(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v(&txn).drop(&mut txn);
    txn.commit()?;
    response.body = sonic_rs::to_vec(&"Success").unwrap();
    Ok(())
}
#[handler]
pub fn print_all(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    tr.v(&txn);
    let nodes = tr.finish()?;
    response.body = sonic_rs::to_vec(&nodes).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct UpdateChatName {
    chat_id: String,
    chat_name: String,
}
#[handler]
pub fn update_chat_name(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let data = sonic_rs::from_slice::<UpdateChatName>(&input.request.body).map_err(|e|GraphError::from(format!("UPDATE ERROR {:?}",e)))?;
    let mut txn = db.graph_env.write_txn().unwrap();
    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    
    tr.v_from_id(&txn, data.chat_id.as_str());
    match tr.current_step {
        TraversalValue::NodeArray(ref nodes) => {
            if nodes.len() == 1 {
                let chat = nodes.first().unwrap();
                if chat.check_property("chat_name") != Some(&Value::String(data.chat_name.clone()))
                {
                    tr.update_props(
                        &mut txn,
                        props! {
                            "chat_name" => data.chat_name,
                        },
                    );
                } else {
                }
            } else {
                return Err(GraphError::from(
                    "More than one node with chat id".to_string(),
                ));
            }
        }
        _ => {
            return Err(GraphError::from("Invalid node".to_string()));
        }
    }

    tr.result(txn)?;

    response.body = sonic_rs::to_vec(&"Success").unwrap();
    Ok(())
}