use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crate::helix_engine::{
    graph_core::{
        ops::{
            g::G,
            in_::{in_e::InEdgesAdapter, to_n::ToNAdapter},
            out::{from_n::FromNAdapter, out::OutAdapter},
            source::{
                add_e::AddE, add_n::AddNAdapter, e::EAdapter, e_from_id::EFromIdAdapter,
                n::NAdapter, n_from_id::NFromIdAdapter,
            },
            tr_val::{Traversable, TraversalVal},
            util::range::RangeAdapter,
        },
        traversal_steps::{SourceTraversalSteps, TraversalBuilderMethods, TraversalSearchMethods},
    },
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use crate::props;
use crate::protocol::{
    filterable::Filterable,
    items::{Edge, Node},
    traversal_value::TraversalValue,
    value::Value,
};
use heed3::RoTxn;
use tempfile::TempDir;

use super::{
    ops::{
        in_::in_::InAdapter,
        out::out_e::OutEdgesAdapter,
        source::{add_e::AddEAdapter, e::E, n::N, n_from_id::NFromId},
        util::filter_ref::FilterRefAdapter,
    },
    traversal::TraversalBuilder,
    traversal_steps::{TraversalMethods, TraversalSteps},
};

fn setup_test_db() -> (Arc<HelixGraphStorage>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let storage = HelixGraphStorage::new(db_path, super::config::Config::default()).unwrap();
    (Arc::new(storage), temp_dir)
}

#[test]
fn test_n() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let thing = storage
        .create_node(&mut txn, "thing", props!(), None, None)
        .unwrap();
    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    // let mut traversal = TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty);
    let nodes = G::new(Arc::clone(&storage), &txn)
        .n()
        .collect_to::<Vec<_>>();
    // Check that the node array contains all nodes
    assert_eq!(nodes.len(), 3);

    let node_ids: Vec<String> = nodes.iter().map(|n| n.id().to_string()).collect();
    let node_labels: Vec<String> = nodes.iter().map(|n| n.label().to_string()).collect();

    assert!(node_ids.contains(&person1.id));
    assert!(node_ids.contains(&person2.id));
    assert!(node_ids.contains(&thing.id));

    assert_eq!(node_labels.iter().filter(|&l| l == "person").count(), 2);
    assert_eq!(node_labels.iter().filter(|&l| l == "thing").count(), 1);
}

#[test]
fn test_e() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Graph Structure:
    // (person1)-[knows]->(person2)
    //         \-[likes]->(person3)
    // (person2)-[follows]->(person3)

    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person3 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    let knows_edge = storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();
    let likes_edge = storage
        .create_edge(&mut txn, "likes", &person1.id, &person3.id, props!())
        .unwrap();
    let follows_edge = storage
        .create_edge(&mut txn, "follows", &person2.id, &person3.id, props!())
        .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e()
        .collect_to::<Vec<_>>();

    // Check that the edge array contains the three edges
    assert_eq!(edges.len(), 3);

    let edge_ids: Vec<String> = edges.iter().map(|e| e.id().to_string()).collect();
    let edge_labels: Vec<String> = edges.iter().map(|e| e.label().to_string()).collect();

    assert!(edge_ids.contains(&knows_edge.id));
    assert!(edge_ids.contains(&likes_edge.id));
    assert!(edge_ids.contains(&follows_edge.id));

    assert!(edge_labels.contains(&"knows".to_string()));
    assert!(edge_labels.contains(&"likes".to_string()));
    assert!(edge_labels.contains(&"follows".to_string()));

    for edge in edges {
        match edge {
            TraversalVal::Edge(edge) => match edge.label() {
                "knows" => {
                    assert_eq!(edge.from_node(), person1.id);
                    assert_eq!(edge.to_node(), person2.id);
                }
                "likes" => {
                    assert_eq!(edge.from_node(), person1.id);
                    assert_eq!(edge.to_node(), person3.id);
                }
                "follows" => {
                    assert_eq!(edge.from_node(), person2.id);
                    assert_eq!(edge.to_node(), person3.id);
                }
                _ => panic!("Unexpected edge label"),
            },
            _ => panic!("Expected Edge value"),
        }
    }
}

#[test]
fn test_n_empty_graph() {
    let (storage, _temp_dir) = setup_test_db();

    let txn = storage.graph_env.read_txn().unwrap();

    let nodes = G::new(Arc::clone(&storage), &txn)
        .n()
        .collect_to::<Vec<_>>();

    // Check that the node array is empty
    assert_eq!(nodes.len(), 0);
}

#[test]
fn test_e_empty_graph() {
    let (storage, _temp_dir) = setup_test_db();

    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e()
        .collect_to::<Vec<_>>();

    // Check that the edge array is empty
    assert_eq!(edges.len(), 0);
}

#[test]
fn test_n_nodes_without_edges() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let nodes = G::new(Arc::clone(&storage), &txn)
        .n()
        .collect_to::<Vec<_>>();

    // Check that the node array contains the two nodes
    assert_eq!(nodes.len(), 2);
    let node_ids: Vec<String> = nodes.iter().map(|n| n.id().to_string()).collect();
    assert!(node_ids.contains(&person1.id));
    assert!(node_ids.contains(&person2.id));
}

#[test]
fn test_add_n() {
    let (storage, _temp_dir) = setup_test_db();

    let mut txn = storage.graph_env.write_txn().unwrap();

    let nodes = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", props! {}, None, None)
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();

    assert_eq!(nodes.first().unwrap().label(), "person");

    // Now txn is free of borrows
    // (If you dropped txn above, you would need to reinitialize it; so in practice, this pattern
    //  is only used if the borrow is the only problem.)

    // If we haven’t dropped txn, ensure no borrows exist before commit
    txn.commit().unwrap();
}

#[test]
fn test_add_e() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let node2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let edges = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", props! {}, None, node1.id.clone(), node2.id.clone())
        .filter_map(|edge| edge.ok())
        .collect::<Vec<_>>();
    txn.commit().unwrap();
    // Check that the current step contains a single edge
    match edges.first() {
        Some(edge) => {
            assert_eq!(edge.label(), "knows");
            match edge {
                TraversalVal::Edge(edge) => {
                    assert_eq!(edge.from_node(), node1.id);
                    assert_eq!(edge.to_node(), node2.id);
                }
                _ => panic!("Expected Edge value"),
            }
        }
        None => panic!("Expected SingleEdge value"),
    }
}

#[test]
fn test_out() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create graph: (person1)-[knows]->(person2)-[knows]->(person3)
    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person3 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();
    storage
        .create_edge(&mut txn, "knows", &person2.id, &person3.id, props!())
        .unwrap();

    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // let nodes = VFromId::new(&storage, &txn, person1.id.as_str())
    //     .out("knows")
    //     .filter_map(|node| node.ok())
    //     .collect::<Vec<_>>();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id(person1.id.as_str())
        .out("knows")
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();

    // txn.commit().unwrap();
    // Check that current step is at person2
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person2.id);
}

// #[test]
// fn test_out_e() {
//     let (storage, _temp_dir) = setup_test_db();
//     let mut txn = storage.graph_env.write_txn().unwrap();

//     // Create graph: (person1)-[knows]->(person2)
//     let person1 = storage
//         .create_node(&mut txn, "person", props!(), None, None)
//         .unwrap();
//     let person2 = storage
//         .create_node(&mut txn, "person", props!(), None, None)
//         .unwrap();

//     let edge = storage
//         .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
//         .unwrap();

//     txn.commit().unwrap();
//     let mut txn = storage.graph_env.write_txn().unwrap();
//     let mut txn = txn.deref();
//     let edges = G::new(Arc::clone(&storage), &mut txn)
//         .n_from_id(person1.id.as_str())
//         .out_edges("knows")
//         .filter_map(|edge| edge.ok())
//         .collect::<Vec<_>>();

//     // Check that current step is at the edge between person1 and person2
//     assert_eq!(edges.len(), 1);
//     assert_eq!(edges[0].id(), edge.id);
//     assert_eq!(edges[0].label(), "knows");
// }

#[test]
fn test_in() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create graph: (person1)-[knows]->(person2)
    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, Some("1".to_string()))
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, Some("2".to_string()))
        .unwrap();

    storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();
    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id(person2.id.as_str())
        .in_("knows")
        .collect_to::<Vec<_>>();

    // Check that current step is at person1
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person1.id);
}

#[test]
fn test_in_e() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph: (person1)-[knows]->(person2)
    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    let edge = storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(person2.id.as_str())
        .in_e("knows")
        .collect_to::<Vec<_>>();

    // Check that current step is at the edge between person1 and person2
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge.id);
    assert_eq!(edges[0].label(), "knows");
}

#[test]
fn test_complex_traversal() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Graph structure:
    // (person1)-[knows]->(person2)-[likes]->(person3)
    //     ^                                     |
    //     |                                     |
    //     +-------<------[follows]------<-------+

    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person3 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();
    storage
        .create_edge(&mut txn, "likes", &person2.id, &person3.id, props!())
        .unwrap();
    storage
        .create_edge(&mut txn, "follows", &person3.id, &person1.id, props!())
        .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();

    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id(person1.id.as_str())
        .out("knows")
        .collect_to::<Vec<_>>();

    // Check that current step is at person2
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person2.id);

    // Traverse from person2 to person3
    let nodes = G::new_from(Arc::clone(&storage), &txn, vec![nodes[0].clone()])
        .out("likes")
        .collect_to::<Vec<_>>();

    // Check that current step is at person3
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person3.id);

    // Traverse from person3 to person1
    let nodes = G::new_from(Arc::clone(&storage), &txn, vec![nodes[0].clone()])
        .out("follows")
        .collect_to::<Vec<_>>();

    // Check that current step is at person1
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person1.id);
}

#[test]
fn test_count_single_node() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let person = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_id(person.id.as_str())
        .count();

    assert_eq!(count, 1);
}

#[test]
fn test_count_node_array() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let _ = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let _ = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let _ = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n() // Get all nodes
        .count();
    assert_eq!(count, 3);
}

#[test]
fn test_count_mixed_steps() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a graph with multiple paths
    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person3 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();
    storage
        .create_edge(&mut txn, "knows", &person1.id, &person3.id, props!())
        .unwrap();
    txn.commit().unwrap();
    println!(
        "person1: {:?},\nperson2: {:?},\nperson3: {:?}",
        person1, person2, person3
    );

    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_id(person1.id.as_str())
        .out("knows")
        .count();

    assert_eq!(count, 2);
}

#[test]
fn test_range_subset() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create multiple nodes
    let _: Vec<Node> = (0..5)
        .map(|_| {
            storage
                .create_node(&mut txn, "person", props!(), None, None)
                .unwrap()
        })
        .collect();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n() // Get all nodes
        .range(1, 3) // Take nodes at index 1 and 2
        .count();

    assert_eq!(count, 2);
}

#[test]
fn test_range_chaining() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create graph: (p1)-[knows]->(p2)-[knows]->(p3)-[knows]->(p4)-[knows]->(p5)
    let nodes: Vec<Node> = (0..5)
        .map(|i| {
            storage
                .create_node(&mut txn, "person", props! { "name" => i }, None, None)
                .unwrap()
        })
        .collect();

    // Create edges connecting nodes sequentially
    for i in 0..4 {
        storage
            .create_edge(&mut txn, "knows", &nodes[i].id, &nodes[i + 1].id, props!())
            .unwrap();
    }

    storage
        .create_edge(&mut txn, "knows", &nodes[4].id, &nodes[0].id, props!())
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n() // Get all nodes
        .range(0, 3) // Take first 3 nodes
        .out("knows") // Get their outgoing nodes
        .collect_to::<Vec<_>>();

    assert_eq!(count.len(), 3);
}

#[test]
fn test_range_empty() {
    let (storage, _temp_dir) = setup_test_db();

    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n() // Get all nodes
        .range(0, 0) // Take first 3 nodes
        .collect_to::<Vec<_>>();

    assert_eq!(count.len(), 0);
}

#[test]
fn test_count_empty() {
    let (storage, _temp_dir) = setup_test_db();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n() // Get all nodes
        .range(0, 0) // Take first 3 nodes
        .count();

    assert_eq!(count, 0);
}

#[test]
fn test_n_from_id() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a test node
    let person = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let node_id = person.id.clone();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node_id)
        .collect_to::<Vec<_>>();

    assert_eq!(count.len(), 1);
}

#[test]
fn test_n_from_id_with_traversal() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph: (person1)-[knows]->(person2)
    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person1.id)
        .out("knows")
        .collect_to::<Vec<_>>();

    // Check that traversal reaches person2
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].id(), person2.id);
}

#[test]
fn test_e_from_id() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph and edge
    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let edge = storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();
    let edge_id = edge.id.clone();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge_id)
        .collect_to::<Vec<_>>();

    // Check that the current step contains the correct single edge
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge_id);
    assert_eq!(edges[0].label(), "knows");
    if let Some(TraversalVal::Edge(edge)) = edges.first() {
        assert_eq!(edge.from_node(), person1.id);
        assert_eq!(edge.to_node(), person2.id);
    } else {
        assert!(false, "Expected Edge value");
    }
}

#[test]
fn test_n_from_id_nonexistent() {
    let (storage, _temp_dir) = setup_test_db();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id("nonexistent_id")
        .collect_to::<Vec<_>>();
    assert!(nodes.is_empty());
}

#[test]
fn test_e_from_id_nonexistent() {
    let (storage, _temp_dir) = setup_test_db();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_id("nonexistent_id")
        .collect_to::<Vec<_>>();
    assert!(edges.is_empty());
}

#[test]
fn test_n_from_id_chain_operations() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph: (person1)-[knows]->(person2)-[likes]->(person3)
    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person3 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();
    storage
        .create_edge(&mut txn, "likes", &person2.id, &person3.id, props!())
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person1.id)
        .out("knows")
        .out("likes")
        .collect_to::<Vec<_>>();

    // Check that the chain of traversals reaches person3
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person3.id);
}

#[test]
fn test_e_from_id_chain_operations() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph and edges
    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person3 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    let edge1 = storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();
    storage
        .create_edge(&mut txn, "likes", &person2.id, &person3.id, props!())
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge1.id)
        .from_n()
        .collect_to::<Vec<_>>();

    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), person1.id);
    assert_eq!(edges[0].label(), "person");
}

#[test]
fn test_filter_nodes() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create nodes with different properties
    let _ = storage
        .create_node(&mut txn, "person", props! { "age" => 25 }, None, None)
        .unwrap();
    let _ = storage
        .create_node(&mut txn, "person", props! { "age" => 30 }, None, None)
        .unwrap();
    let person3 = storage
        .create_node(&mut txn, "person", props! { "age" => 35 }, None, None)
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let traversal = G::new(Arc::clone(&storage), &txn)
        .n()
        .filter_ref(|val, _| {
            println!("val: {:?}", val);
            if let Ok(TraversalVal::Node(node)) = val {
                if let Some(value) = node.check_property("age") {
                    match value {
                        Value::Float(age) => *age > 30.0,
                        Value::Integer(age) => *age > 30,
                        _ => false,
                    }
                } else {
                    false
                }
            } else {
                false
            }
        })
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person3.id);
}

#[test]
fn test_filter_macro_single_argument() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = storage
        .create_node(&mut txn, "person", props! { "name" => "Alice" }, None, None)
        .unwrap();
    let _ = storage
        .create_node(&mut txn, "person", props! { "name" => "Bob" }, None, None)
        .unwrap();

    fn has_name(val: &Result<TraversalVal, GraphError>) -> bool {
        if let Ok(TraversalVal::Node(node)) = val {
            return node.check_property("name").is_some();
        } else {
            return false;
        }
    }

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n()
        .filter_ref(|val, _| has_name(val))
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 2);
    assert!(traversal
        .iter()
        .any(|val| if let TraversalVal::Node(node) = val {
            let name = node.check_property("name").unwrap();
            name == &Value::String("Alice".to_string()) || name == &Value::String("Bob".to_string())
        } else {
            false
        }));
}

#[test]
fn test_filter_macro_multiple_arguments() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = storage
        .create_node(&mut txn, "person", props! { "age" => 25 }, None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props! { "age" => 30 }, None, None)
        .unwrap();
    txn.commit().unwrap();

    fn age_greater_than(val: &Result<TraversalVal, GraphError>, min_age: i32) -> bool {
        if let Ok(TraversalVal::Node(node)) = val {
            if let Some(value) = node.check_property("age") {
                match value {
                    Value::Float(age) => *age > min_age as f64,
                    Value::Integer(age) => *age > min_age,
                    _ => false,
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n()
        .filter_ref(|val, _| age_greater_than(val, 27))
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person2.id);
}

#[test]
fn test_filter_edges() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();
    let person2 = storage
        .create_node(&mut txn, "person", props!(), None, None)
        .unwrap();

    let _ = storage
        .create_edge(
            &mut txn,
            "knows",
            &person1.id,
            &person2.id,
            props! { "since" => 2020 },
        )
        .unwrap();
    let edge2 = storage
        .create_edge(
            &mut txn,
            "knows",
            &person2.id,
            &person1.id,
            props! { "since" => 2022 },
        )
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    fn recent_edge(val: &Result<TraversalVal, GraphError>, year: i32) -> bool {
        if let Ok(TraversalVal::Edge(edge)) = val {
            if let Some(value) = edge.check_property("since") {
                match value {
                    Value::Integer(since) => return *since > year,
                    Value::Float(since) => return *since > year as f64,
                    _ => return false,
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    let traversal = G::new(Arc::clone(&storage), &txn)
        .e()
        .filter_ref(|val, _| recent_edge(val, 2021))
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), edge2.id);
}

#[test]
fn test_filter_empty_result() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = storage
        .create_node(&mut txn, "person", props! { "age" => 25 }, None, None)
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n()
        .filter_ref(|val, _| {
            if let Ok(TraversalVal::Node(node)) = val {
                if let Some(value) = node.check_property("age") {
                    match value {
                        Value::Integer(age) => return *age > 100,
                        Value::Float(age) => return *age > 100.0,
                        _ => return false,
                    }
                } else {
                    false
                }
            } else {
                false
            }
        })
        .collect_to::<Vec<_>>();
    assert!(traversal.is_empty());
}

#[test]
fn test_filter_chain() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = storage
        .create_node(
            &mut txn,
            "person",
            props! { "age" => 25, "name" => "Alice" },
            None,
            None,
        )
        .unwrap();
    let person2 = storage
        .create_node(
            &mut txn,
            "person",
            props! { "age" => 30, "name" => "Bob" },
            None,
            None,
        )
        .unwrap();
    let _ = storage
        .create_node(&mut txn, "person", props! { "age" => 35 }, None, None)
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    fn has_name(val: &Result<TraversalVal, GraphError>) -> bool {
        if let Ok(TraversalVal::Node(node)) = val {
            return node.check_property("name").is_some();
        } else {
            return false;
        }
    }

    fn age_greater_than(val: &Result<TraversalVal, GraphError>, min_age: i32) -> bool {
        if let Ok(TraversalVal::Node(node)) = val {
            if let Some(value) = node.check_property("age") {
                match value {
                    Value::Float(age) => return *age > min_age as f64,
                    Value::Integer(age) => return *age > min_age,
                    _ => return false,
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    let traversal = G::new(Arc::clone(&storage), &txn)
        .n()
        .filter_ref(|val, _| has_name(val))
        .filter_ref(|val, _| age_greater_than(val, 27))
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person2.id);
}

#[test]
fn test_in_n() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = storage
        .create_node(
            &mut txn,
            "person",
            props!(),
            None,
            Some("person1".to_string()),
        )
        .unwrap();
    let person2 = storage
        .create_node(
            &mut txn,
            "person",
            props!(),
            None,
            Some("person2".to_string()),
        )
        .unwrap();

    let edge = storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge.id)
        .to_n()
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person2.id);
}

#[test]
fn test_out_n() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = storage
        .create_node(
            &mut txn,
            "person",
            props!(),
            None,
            Some("person1".to_string()),
        )
        .unwrap();
    let person2 = storage
        .create_node(
            &mut txn,
            "person",
            props!(),
            None,
            Some("person2".to_string()),
        )
        .unwrap();

    let edge = storage
        .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge.id)
        .from_n()
        .collect_to::<Vec<_>>();
    println!("traversal: {:?}", traversal);
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person1.id);
}

// #[test]
// fn test_shortest_mutual_path() {
//     let (storage, _temp_dir) = setup_test_db();
//     let mut txn = storage.graph_env.write_txn().unwrap();

//     // Create a complex network of mutual and one-way connections
//     // Mutual: Alice <-> Bob <-> Charlie <-> David
//     // One-way: Alice -> Eve -> David
//     let users: Vec<Node> = vec!["alice", "bob", "charlie", "dave", "eve"]
//         .iter()
//         .map(|name| {
//             storage
//                 .create_node(&mut txn, "person", props! { "name" => *name }, None, None)
//                 .unwrap()
//         })
//         .collect();

//     for (i, j) in [(0, 1), (1, 2), (2, 3)].iter() {
//         storage
//             .create_edge(&mut txn, "knows", &users[*i].id, &users[*j].id, props!())
//             .unwrap();
//         storage
//             .create_edge(&mut txn, "knows", &users[*j].id, &users[*i].id, props!())
//             .unwrap();
//     }

//     storage
//         .create_edge(&mut txn, "knows", &users[0].id, &users[4].id, props!())
//         .unwrap();
//     storage
//         .create_edge(&mut txn, "knows", &users[4].id, &users[3].id, props!())
//         .unwrap();

//     txn.commit().unwrap();

//     let txn = storage.graph_env.read_txn().unwrap();
//     let mut tr =
//         TraversalBuilder::new(Arc::clone(&storage), TraversalValue::from(users[0].clone()));
//     tr.shortest_mutual_path_to(&txn, &users[3].id);

//     let result = tr.result(txn);
//     let paths = match result.unwrap() {
//         TraversalValue::Paths(paths) => paths,
//         _ => {
//             panic!("Expected PathArray value")
//         }
//     };

//     assert_eq!(paths.len(), 1);
//     let (nodes, edges) = &paths[0];

//     assert_eq!(nodes.len(), 4);
//     assert_eq!(edges.len(), 3);
//     assert_eq!(nodes[0].id, users[3].id); // David
//     assert_eq!(nodes[1].id, users[2].id); // Charlie
//     assert_eq!(nodes[2].id, users[1].id); // Bob
//     assert_eq!(nodes[3].id, users[0].id); // Alice
// }
