use std::sync::Arc;

use heed3::{types::Bytes, RoTxn};

use crate::{
    decode_str,
    helix_engine::storage_core::{
        storage_core::HelixGraphStorage, storage_methods::StorageMethods,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

use super::tr_val::{Traversable, TraversalVal};

pub struct InNodesIterator<'a> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    edge_label: &'a str,
}

// implementing iterator for InNodesIterator
impl<'a> Iterator for InNodesIterator<'a> {
    type Item = TraversalVal;

    /// Returns the next incoming node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, value))) = self.iter.next() {
            let edge_id = std::str::from_utf8(value.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, edge_id) {
                if self.edge_label.is_empty() || edge.label == self.edge_label {
                    if let Ok(node) = self.storage.get_node(self.txn, &edge.to_node) {
                        return Some(TraversalVal::Node(node));
                    }
                }
            }
        }
        None
    }
}

pub struct InNodes<'a, I: Iterator<Item = TraversalVal>, F>
where
    F: FnMut(TraversalVal) -> InNodesIterator<'a>,
{
    iter: std::iter::Flatten<std::iter::Map<I, F>>,
}

impl<'a, I, F> Iterator for InNodes<'a, I, F>
where
    I: Iterator<Item = TraversalVal>,
    F: FnMut(TraversalVal) -> InNodesIterator<'a>,
{
    type Item = TraversalVal;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait InAdapter: Iterator {
    fn in_<'a>(
        self,
        db: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
        edge_label: &'a str,
    ) -> InNodes<'a, Self, impl FnMut(TraversalVal) -> InNodesIterator<'a>>
    where
        Self: Sized + Iterator<Item = TraversalVal> + 'a,
        Self::Item: Send,
    {
        // iterate through the iterator and create a new iterator on the out edges
        let db = Arc::clone(&db);
        let iter = self
            .map(move |item| in_nodes(item, db.clone(), txn, edge_label))
            .flatten();
        InNodes { iter }
    }
}

/// Returns an iterator over the out nodes of the given node
pub fn in_nodes<'a>(
    item: TraversalVal,
    db: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    edge_label: &'a str,
) -> InNodesIterator<'a> {
    let prefix = HelixGraphStorage::out_edge_key(item.id(), "");
    let iter = db
        .out_edges_db
        .lazily_decode_data()
        .prefix_iter(txn, &prefix)
        .unwrap();

    InNodesIterator {
        iter,
        storage: db,
        txn,
        edge_label,
    }
}

impl<T: ?Sized> InAdapter for T where T: Iterator {}
