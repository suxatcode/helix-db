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
        items::{Edge, SerializedEdge}, label_hash::hash_label,
    },
};

use super::super::tr_val::TraversalVal;

pub struct EFromTypes<'a> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Unit>>,
    storage: &'a Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    length: usize,
}
// implementing iterator for OutIterator
impl<'a> Iterator for EFromTypes<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|value| {
            let (key, _) = value.unwrap();
            let edge_id = HelixGraphStorage::get_u128_from_bytes(&key[self.length..])?;
            let edge = match self.storage.get_edge(self.txn, &edge_id) {
                Ok(edge) => edge,
                Err(e) => return Err(e),
            };
            Ok(TraversalVal::Edge(edge))
        })
    }
}

impl<'a> EFromTypes<'a> {
    pub fn new(storage: &'a Arc<HelixGraphStorage>, txn: &'a RoTxn, label: &str) -> Self {
        let label_hash = hash_label(label, None);
        let prefix = HelixGraphStorage::edge_label_key(&label_hash, None);
        let iter = storage
            .edge_labels_db
            .lazily_decode_data()
            .prefix_iter(&txn, &prefix)
            .unwrap();
        EFromTypes {
            iter,
            storage: &storage,
            txn: &txn,
            length: prefix.len(),
        }
    }
}
