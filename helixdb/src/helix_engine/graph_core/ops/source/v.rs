use std::sync::Arc;

use heed3::{
    types::{Bytes, Lazy},
    RoTxn,
};

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RoTraversalIterator,
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

use super::super::tr_val::TraversalVal;

pub struct V<'a> {
    iter: heed3::RoIter<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
}

// implementing iterator for OutIterator
impl<'a> Iterator for V<'a> {
    type Item = TraversalVal;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|value| {
            let (_, value) = value.unwrap();
            let value = value.decode().unwrap();
            if !value.is_empty() {
                match bincode::deserialize(&value) {
                    Ok(node) => TraversalVal::Node(node),
                    Err(e) => {
                        println!("Error deserializing node: {}", e);
                        TraversalVal::Empty
                    }
                }
            } else {
                TraversalVal::Empty
            }
        })
    }
}

impl<'a> V<'a> {
    pub fn new(storage: Arc<HelixGraphStorage>, txn: &'a RoTxn) -> RoTraversalIterator<'a, Self> {
        let iter = storage.nodes_db.lazily_decode_data().iter(txn).unwrap();

        // Create the base V iterator
        let v_iter = V { iter };

        // Wrap it with the RoTraversalIterator adapter
        RoTraversalIterator {
            inner: v_iter,
            storage: storage.clone(),
            txn,
        }
    }
}
