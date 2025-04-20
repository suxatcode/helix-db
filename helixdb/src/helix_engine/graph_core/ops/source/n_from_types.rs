use std::sync::Arc;

use heed3::{
    types::{Bytes, Lazy, Unit},
    RoTxn,
};

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RoTraversalIterator,
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

use super::super::tr_val::TraversalVal;

pub struct NFromTypes<'a> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Unit>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    length: usize,
}
// implementing iterator for OutIterator
impl<'a> Iterator for NFromTypes<'a> {
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
pub trait NFromTypesAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn n_from_types(
        self,
        types: &'a [&'a str],
    ) -> impl Iterator<Item = Result<TraversalVal, GraphError>>;
}
impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> NFromTypesAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn n_from_types(
        self,
        types: &'a [&'a str],
    ) -> impl Iterator<Item = Result<TraversalVal, GraphError>> {
        let db = self.storage.clone();
        let txn: &RoTxn<'_> = self.txn;
        let iter = types.iter().flat_map(move |label| {
            let prefix = HelixGraphStorage::node_label_key(label, "");
            let iter = db
                .node_labels_db
                .lazily_decode_data()
                .prefix_iter(&self.txn, &prefix)
                .unwrap();
            NFromTypes {
                iter,
                storage: db.clone(),
                txn,
                length: prefix.len(),
            }
        });
        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
