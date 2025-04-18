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

use super::super::tr_val::{Traversable, TraversalVal};

pub struct OutNodesIterator<'a> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    edge_label: &'a str,
}

// implementing iterator for OutIterator
impl<'a> Iterator for OutNodesIterator<'a> {
    type Item = TraversalVal;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
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

pub struct OutNodes<'a, I: Iterator<Item = TraversalVal>, F>
where
    F: FnMut(TraversalVal) -> OutNodesIterator<'a>,
{
    iter: std::iter::Flatten<std::iter::Map<I, F>>,
}

impl<'a, I, F> Iterator for OutNodes<'a, I, F>
where
    I: Iterator<Item = TraversalVal>,
    F: FnMut(TraversalVal) -> OutNodesIterator<'a>,
{
    type Item = TraversalVal;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait OutAdapter: Iterator {
    fn out<'a>(
        self,
        db: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
        edge_label: &'a str,
    ) -> OutNodes<'a, Self, impl FnMut(TraversalVal) -> OutNodesIterator<'a>>
    where
        Self: Sized + Iterator<Item = TraversalVal> + 'a,
        Self::Item: Send,
    {
        // iterate through the iterator and create a new iterator on the out edges
        let db = Arc::clone(&db);
        let iter = self
            .map(move |item| out_nodes(item, db.clone(), txn, edge_label))
            .flatten();
        OutNodes { iter }
    }
}

/// Returns an iterator over the out nodes of the given node
pub fn out_nodes<'a>(
    item: TraversalVal,
    db: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    edge_label: &'a str,
) -> OutNodesIterator<'a> {
    let prefix = HelixGraphStorage::out_edge_key(item.id(), "");
    let iter = db
        .out_edges_db
        .lazily_decode_data()
        .prefix_iter(txn, &prefix)
        .unwrap();

    OutNodesIterator {
        iter,
        storage: db,
        txn,
        edge_label,
    }
}

impl<T: ?Sized> OutAdapter for T where T: Iterator {}
