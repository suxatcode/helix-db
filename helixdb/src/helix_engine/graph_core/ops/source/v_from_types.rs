use std::sync::Arc;

use heed3::{
    types::{Bytes, Lazy, Unit},
    RoTxn,
};

use crate::{
    helix_engine::{
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

use super::super::tr_val::TraversalVal;

pub struct VFromTypes<'a> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Unit>>,
    storage: &'a Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    length: usize,
}
// implementing iterator for OutIterator
impl<'a> Iterator for VFromTypes<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|value| {
            let (key, _) = value.unwrap();
            let node_id = std::str::from_utf8(&key[self.length..])?;
            let node: Node = match self.storage.get_node(self.txn, node_id) {
                Ok(node) => node,
                Err(e) => return Err(e),
            };
            Ok(TraversalVal::Node(node))
        })
    }
}

impl<'a> VFromTypes<'a> {
    pub fn new(storage: &'a Arc<HelixGraphStorage>, txn: &'a RoTxn, label: &str) -> Self {
        let prefix = HelixGraphStorage::node_label_key(label, "");
        let iter = storage
            .node_labels_db
            .lazily_decode_data()
            .prefix_iter(&txn, &prefix)
            .unwrap();
        VFromTypes {
            iter,
            storage: &storage,
            txn: &txn,
            length: prefix.len(),
        }
    }
}
