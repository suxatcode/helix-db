use std::sync::Arc;

use heed3::{
    types::{Bytes, Lazy},
    RoTxn,
};

use crate::{
    helix_engine::storage_core::{
        storage_core::HelixGraphStorage, storage_methods::StorageMethods,
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
                let node: Node = bincode::deserialize(&value).unwrap();
                TraversalVal::Node(node)
            } else {
                TraversalVal::Empty
            }
        })
    }
}

impl<'a> V<'a> {
    pub fn new(storage: &'a Arc<HelixGraphStorage>, txn: &'a RoTxn) -> Self {
        let iter = storage.nodes_db.lazily_decode_data().iter(txn).unwrap();
        V { iter }
    }
}
