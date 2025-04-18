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

pub struct InEdgesIterator<'a> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    edge_label: &'a str,
}

// implementing iterator for InEdgesIterator
impl<'a> Iterator for InEdgesIterator<'a> {
    type Item = TraversalVal;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, value))) = self.iter.next() {
            let edge_id = std::str::from_utf8(value.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, edge_id) {
                if self.edge_label.is_empty() || edge.label == self.edge_label {
                    return Some(TraversalVal::Edge(edge));
                }
            }
        }
        None
    }
}

pub struct InEdges<'a, I: Iterator<Item = TraversalVal>, F>
where
    F: FnMut(TraversalVal) -> InEdgesIterator<'a>,
{
    iter: std::iter::Flatten<std::iter::Map<I, F>>,
}

impl<'a, I, F> Iterator for InEdges<'a, I, F>
where
    I: Iterator<Item = TraversalVal>,
    F: FnMut(TraversalVal) -> InEdgesIterator<'a>,
{
    type Item = TraversalVal;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait InEdgesAdapter: Iterator {
    fn in_edges<'a>(
        self,
        db: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
        edge_label: &'a str,
    ) -> InEdges<'a, Self, impl FnMut(TraversalVal) -> InEdgesIterator<'a>>
    where
        Self: Sized + Iterator<Item = TraversalVal> + 'a,
        Self::Item: Send,
    {
        // iterate through the iterator and create a new iterator on the in edges
        let db = Arc::clone(&db);
        let iter = self
            .map(move |item| in_edges(item, db.clone(), txn, edge_label))
            .flatten();
        InEdges { iter }
    }
}

/// Returns an iterator over the in edges of the given node
pub fn in_edges<'a>(
    item: TraversalVal,
    db: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    edge_label: &'a str,
) -> InEdgesIterator<'a> {
    let prefix = HelixGraphStorage::out_edge_key(item.id(), "");
    let iter = db
        .in_edges_db
        .lazily_decode_data()
        .prefix_iter(txn, &prefix)
        .unwrap();

    InEdgesIterator {
        iter,
        storage: db,
        txn,
        edge_label,
    }
}

impl<T: ?Sized> InEdgesAdapter for T where T: Iterator {}
