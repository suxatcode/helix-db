use std::sync::Arc;

use heed3::types::{Bytes, Lazy};
use tempfile::TempDir;

use crate::{
    helix_engine::{
        graph_core::{
            config::Config,
            ops::{
                source::{add_v::AddN, v::V},
                tr_val::TraversalVal,
                util::{filter_ref::FilterRefAdapter, range::RangeAdapter},
                *,
            },
        },
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    },
    props,
    protocol::value::Value,
};

fn setup_temp_db() -> HelixGraphStorage {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let storage = HelixGraphStorage::new(db_path, Config::default()).unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    storage
        .create_node(
            &mut txn,
            "person",
            props! { "name" => "xav"},
            None,
            Some("1".to_string()),
        )
        .unwrap();
    storage
        .create_node(
            &mut txn,
            "person",
            props! {"name" => "gog"},
            None,
            Some("2".to_string()),
        )
        .unwrap();
    storage
        .create_edge(&mut txn, "follows", "1", "2", props! {})
        .unwrap();

    txn.commit().unwrap();

    storage
}

#[test]
fn test_new_out() {
    let db = setup_temp_db();
    let db = Arc::new(db);
    let mut txn = db.graph_env.write_txn().unwrap();
    let reference = &mut txn;
    let res = AddN::new(
        &db,
        &mut txn,
        "person",
        props! { "name" => "xav"},
        None,
        Some("3".to_string()),
    )
    .filter_ref(&txn, |item, txn| {
        if let Ok(TraversalVal::Node(node)) = item {
            match node.properties.get("name").unwrap() {
                Value::String(st) => st == "xav",
                _ => false,
            }
        } else {
            false
        }
    })
    .filter_map(|x| x.ok())
    .collect::<Vec<_>>();

    // let res = V::new(&db, &txn)
    //     .filter_ref(&txn, |item, txn| {
    //         if let TraversalVal::Node(node) = item {
    //             match node.properties.get("name").unwrap() {
    //                 Value::String(st) => st == "xav",
    //                 _ => false,
    //             }
    //         } else {
    //             false
    //         }
    //     })
    //     .range(0, 4)
    //     .collect::<Vec<_>>();

    println!("{:?}", res);
    assert!(false);
    return;
}
