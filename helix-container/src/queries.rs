use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::helix_engine::graph_core::ops::source::add_e::EdgeType;
use helixdb::helix_engine::graph_core::ops::util::range;
use helixdb::helix_engine::vector_core::hnsw::HNSW;
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::AddEAdapter, add_n::AddNAdapter, e::EAdapter, e_from_id::EFromId,
            e_from_types::EFromTypes, n::NAdapter, n_from_id::NFromId,
            n_from_types::NFromTypesAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, drop::DropAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, range::RangeAdapter, update::Update,
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

#[handler]
pub fn hnswload(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct hnswloadData {
        vectors: Vec<Vec<f64>>,
    }

    let data: hnswloadData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let tr = G::new_mut(Arc::clone(&db), &mut txn)
        .insert_vs::<fn(&HVector) -> bool>(&data.vectors, None);
    let res = tr.collect_to::<Vec<_>>();

    let tr = G::new_from(Arc::clone(&db), &txn, res.clone());
    let tr = tr
        .map(|item| {
            match item {
                Ok(ref item) => {
                    let id = item.check_property("ID");
                    let id_remapping = Remapping::new(
                        false,
                        None,
                        Some(match id {
                            Some(value) => ReturnValue::from(value.clone()),
                            None => {
                                return Err(GraphError::ConversionError(
                                    "Property not found on id".to_string(),
                                ))
                            }
                        }),
                    );
                    remapping_vals.borrow_mut().insert(
                        item.id().clone(),
                        ResponseRemapping::new(
                            HashMap::from([("ID".to_string(), id_remapping)]),
                            false,
                        ),
                    );
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                    return Err(GraphError::ConversionError("Error: {:?}".to_string()));
                }
            };
            item
        })
        .filter_map(|item| item.ok());
    let return_val = tr.collect::<Vec<_>>();
    return_vals.insert(
        "res".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(return_val, remapping_vals.borrow_mut()),
    );
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    txn.commit()?;
    Ok(())
}

#[handler]
pub fn hnswinsert(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct hnswinsertData {
        vector: Vec<f64>,
    }

    let data: hnswinsertData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let tr =
        G::new_mut(Arc::clone(&db), &mut txn).insert_v::<fn(&HVector) -> bool>(&data.vector, None);
    let res = tr.collect_to::<Vec<_>>();

    let tr = G::new_from(Arc::clone(&db), &txn, res.clone());
    let tr = tr
        .map(|item| {
            match item {
                Ok(ref item) => {
                    let id = item.check_property("ID");
                    let id_remapping = Remapping::new(
                        false,
                        None,
                        Some(match id {
                            Some(value) => ReturnValue::from(value.clone()),
                            None => {
                                return Err(GraphError::ConversionError(
                                    "Property not found on id".to_string(),
                                ))
                            }
                        }),
                    );
                    remapping_vals.borrow_mut().insert(
                        item.id().clone(),
                        ResponseRemapping::new(
                            HashMap::from([("ID".to_string(), id_remapping)]),
                            false,
                        ),
                    );
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                    return Err(GraphError::ConversionError("Error: {:?}".to_string()));
                }
            };
            item
        })
        .filter_map(|item| item.ok());
    let return_val = tr.collect::<Vec<_>>();
    return_vals.insert(
        "res".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(return_val, remapping_vals.borrow_mut()),
    );
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    txn.commit()?;
    Ok(())
}

#[handler]
pub fn hnswsearch(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct hnswsearchData {
        query: Vec<f64>,
        k: i32,
    }

    let data: hnswsearchData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let tr = G::new(Arc::clone(&db), &txn).search_v::<fn(&HVector) -> bool>(
        &data.query,
        data.k as usize,
        None,
    );
    let res = tr.collect_to::<Vec<_>>();

    return_vals.insert(
        "res".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(res, remapping_vals.borrow_mut()),
    );
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn ragloaddocs(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct ragloaddocsData {
        docs: Vec<docsData>,
    }

    #[derive(Serialize, Deserialize)]
    struct docsData {
        doc: String,
        vecs: Vec<Vec<f64>>,
    }

    let data: ragloaddocsData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    for data in data.docs {
        let tr = G::new_mut(Arc::clone(&db), &mut txn).add_n(
            "Type",
            props! { "content".to_string() => data.doc },
            None,
            None,
        );
        let doc_node = tr.collect_to::<Vec<_>>();

        let tr = G::new_mut(Arc::clone(&db), &mut txn)
            .insert_vs::<fn(&HVector) -> bool>(&data.vecs, None);
        let vectors = tr.collect_to::<Vec<_>>();

        for data in vectors {
            // TODO: needs to be vectors not data.vecs
            let tr = G::new_mut(Arc::clone(&db), &mut txn).add_e(
                "Contains",
                props! {},
                None,
                doc_node.id(),
                data.id(),
                false,
                EdgeType::Vec,
            ); // TODO: from_is_vec, to_is_vec
            let _ = tr.collect_to::<Vec<_>>();
        }
    }
    return_vals.insert("message".to_string(), ReturnValue::from("Success"));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    txn.commit()?;
    Ok(())
}

#[handler]
pub fn ragsearchdoc(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct ragsearchdocData {
        query: Vec<f64>,
    }

    let data: ragsearchdocData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let tr = G::new(Arc::clone(&db), &txn).search_v::<fn(&HVector) -> bool>(&data.query, 1, None);
    let vec = tr.collect_to::<Vec<_>>();

    let tr = G::new_from(Arc::clone(&db), &txn, vec.clone()).in_("Contains");
    let doc_node = tr.collect_to::<Vec<_>>();

    let tr = G::new_from(Arc::clone(&db), &txn, doc_node.clone());
    let tr = tr
        .map(|item| {
            match item {
                Ok(ref item) => {
                    let content = item.check_property("content");
                    let content_remapping = Remapping::new(
                        false,
                        None,
                        Some(match content {
                            Some(value) => ReturnValue::from(value.clone()),
                            None => {
                                return Err(GraphError::ConversionError(
                                    "Property not found on content".to_string(),
                                ))
                            }
                        }),
                    );
                    remapping_vals.borrow_mut().insert(
                        item.id().clone(),
                        ResponseRemapping::new(
                            HashMap::from([("content".to_string(), content_remapping)]),
                            false,
                        ),
                    );
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                    return Err(GraphError::ConversionError("Error: {:?}".to_string()));
                }
            };
            item
        })
        .filter_map(|item| item.ok());
    let return_val = tr.collect::<Vec<_>>();
    return_vals.insert(
        "doc_node".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(return_val, remapping_vals.borrow_mut()),
    );
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    Ok(())
}

#[handler]
pub fn ragtestload(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    #[derive(Serialize, Deserialize)]
    struct ragtestloadData {
        doc: String,
        vec: Vec<f64>,
    }

    let data: ragtestloadData = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> =
        RefCell::new(HashMap::new());
    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().unwrap();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(1);

    let tr = G::new_mut(Arc::clone(&db), &mut txn).add_n(
        "Type",
        props! { "content".to_string() => data.doc },
        None,
        None,
    );
    let doc_node = tr.collect_to::<Vec<_>>();

    let tr =
        G::new_mut(Arc::clone(&db), &mut txn).insert_v::<fn(&HVector) -> bool>(&data.vec, None);
    let vector = tr.collect_to::<Vec<_>>(); // TODO: vector

    let tr = G::new_mut(Arc::clone(&db), &mut txn).add_e(
        "Contains",
        props! {},
        None,
        doc_node.id(),
        vector.id(),
        false,
        EdgeType::Vec,
    ); // TODO: need to add from_is_vec, to_is_vec
       // - and should'nt be data.id() but vector.id()
    let _ = tr.collect_to::<Vec<_>>();

    return_vals.insert("message".to_string(), ReturnValue::from("Success"));
    response.body = sonic_rs::to_vec(&return_vals).unwrap();

    txn.commit()?;

    let mut count = 0;

    let mut txn = db.graph_env.read_txn().unwrap();

    let count = db.vectors.vectors_db.iter(&txn).unwrap().count();
    println!("count: {:?}", count);

    //for i in db.vectors.get_all_vectors(&txn, Some(0)) {
    //}

    Ok(())
}
