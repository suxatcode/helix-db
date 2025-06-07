

use heed3::RoTxn;
use get_routes::handler;
use helixdb::{field_remapping, identifier_remapping, traversal_remapping, exclude_field};
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::{AddEAdapter, EdgeType},
            add_n::AddNAdapter,
            e_from_id::EFromIdAdapter,
            e_from_type::EFromTypeAdapter,
            n_from_id::NFromIdAdapter,
            n_from_type::NFromTypeAdapter,
            n_from_index::NFromIndexAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, range::RangeAdapter, update::UpdateAdapter,
            map::MapAdapter, paths::ShortestPathAdapter, props::PropsAdapter, drop::Drop,
        },
        vectors::{insert::InsertVAdapter, search::SearchVAdapter, brute_force_search::BruteForceSearchVAdapter},
        bm25::search_bm25::SearchBM25Adapter,
        
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::remapping::ResponseRemapping,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::{
        filterable::Filterable, remapping::Remapping, return_values::ReturnValue, value::Value, id::ID,
    },
};
use sonic_rs::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use std::cell::RefCell;
use chrono::{DateTime, Utc};
    
pub struct User {
    pub name: String,
    pub age: u32,
    pub email: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct Post {
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct Follows {
    pub from: User,
    pub to: User,
    pub since: DateTime<Utc>,
}

pub struct Created {
    pub from: User,
    pub to: Post,
    pub created_at: DateTime<Utc>,
}


#[handler]
pub fn GetPosts (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let posts = G::new(Arc::clone(&db), &txn)
.n_from_type("Post").collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("posts".to_string(), ReturnValue::from_traversal_value_array_with_mixin(posts.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct CreatePostInput {

pub user_id: ID,
pub content: String
}
#[handler]
pub fn CreatePost (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: CreatePostInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let user = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.user_id).collect_to::<Vec<_>>();
    let post = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("Post", Some(props! { "content" => data.content.clone(), "created_at" => chrono::Utc::now().to_rfc3339(), "updated_at" => chrono::Utc::now().to_rfc3339() }), None).collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&db), &mut txn)
.add_e("Created", None, user.id(), post.id(), true, EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("post".to_string(), ReturnValue::from_traversal_value_array_with_mixin(post.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct GetFollowedUsersInput {

pub user_id: ID
}
#[handler]
pub fn GetFollowedUsers (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: GetFollowedUsersInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let followed = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.user_id)

.out("Follows",&EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("followed".to_string(), ReturnValue::from_traversal_value_array_with_mixin(followed.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct CreateFollowInput {

pub follower_id: ID,
pub followed_id: ID
}
#[handler]
pub fn CreateFollow (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: CreateFollowInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let follower = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.follower_id).collect_to::<Vec<_>>();
    let followed = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.followed_id).collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&db), &mut txn)
.add_e("Follows", None, follower.id(), *data.followed_id, true, EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("success".to_string(), ReturnValue::from(Value::from("success")));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn GetUsers (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let users = G::new(Arc::clone(&db), &txn)
.n_from_type("User").collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("users".to_string(), ReturnValue::from_traversal_value_array_with_mixin(users.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct GetFollowedUsersPostsInput {

pub user_id: ID
}
#[handler]
pub fn GetFollowedUsersPosts (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: GetFollowedUsersPostsInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let followers = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.user_id)

.out("Follows",&EdgeType::Node).collect_to::<Vec<_>>();
    let posts = G::new_from(Arc::clone(&db), &txn, followers.clone())

.out("Created",&EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("posts".to_string(), ReturnValue::from_traversal_value_array_with_mixin(G::new_from(Arc::clone(&db), &txn, posts.clone())

.map_traversal(|item, txn| { traversal_remapping!(remapping_vals, item.clone(), "post" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.check_property("content").collect_to::<Vec<_>>())?;
traversal_remapping!(remapping_vals, item.clone(), "creatorID" => G::new_from(Arc::clone(&db), &txn, vec![item.clone()])

.in_("Created",&EdgeType::Node)

.check_property("id").collect_to::<Vec<_>>())?;
 Ok(item) }).collect_to::<Vec<_>>().clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct GetPostsByUserInput {

pub user_id: ID
}
#[handler]
pub fn GetPostsByUser (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: GetPostsByUserInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let txn = db.graph_env.read_txn().unwrap();
    let posts = G::new(Arc::clone(&db), &txn)
.n_from_id(&data.user_id)

.out("Created",&EdgeType::Node).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("posts".to_string(), ReturnValue::from_traversal_value_array_with_mixin(posts.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct CreateUserInput {

pub name: String,
pub age: u32,
pub email: String
}
#[handler]
pub fn CreateUser (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
let data: CreateUserInput = match sonic_rs::from_slice(&input.request.body) {
    Ok(data) => data,
    Err(err) => return Err(GraphError::from(err)),
};

let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());
let db = Arc::clone(&input.graph.storage);
let mut txn = db.graph_env.write_txn().unwrap();
    let user = G::new_mut(Arc::clone(&db), &mut txn)
.add_n("User", Some(props! { "updated_at" => chrono::Utc::now().to_rfc3339(), "age" => data.age.clone(), "created_at" => chrono::Utc::now().to_rfc3339(), "name" => data.name.clone(), "email" => data.email.clone() }), None).collect_to::<Vec<_>>();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("user".to_string(), ReturnValue::from_traversal_value_array_with_mixin(user.clone(), remapping_vals.borrow_mut()));

    txn.commit().unwrap();
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}
