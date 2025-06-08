use std::sync::Arc;

use crate::{
    helix_engine::{
        graph_core::{
            ops::tr_val::{Traversable, TraversalVal},
            traversal_iter::RoTraversalIterator,
        },
        types::GraphError,
    },
    helix_storage::Storage,
    protocol::{items::Edge, label_hash::hash_label},
};

pub struct InEdgesIterator<S: Storage + ?Sized> {
    pub edges: Vec<Edge>,
    pub storage: Arc<S>,
}

impl<S: Storage + ?Sized> Iterator for InEdgesIterator<S> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.edges
            .pop()
            .map(|edge| Ok(TraversalVal::Edge(edge)))
    }
}

pub trait InEdgesAdapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    /// Returns an iterator containing the edges that have an incoming edge with the given label.
    ///
    /// Note that the `edge_label` cannot be empty and must be a valid, existing edge label.
    ///
    /// To provide safety, you cannot get all incoming edges as it would be ambiguous as to what
    /// type that resulting edge would be.  
    fn in_e(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, S> InEdgesAdapter<'a, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    #[inline]
    fn in_e(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let storage = Arc::clone(&self.storage);
        let txn = self.txn;
        let iter = self
            .inner
            .filter_map(move |item| match item {
                Ok(item) => Some(item.id()),
                Err(_) => None,
            })
            .map(move |id| self.storage.get_in_edges(txn, edge_label, &id))
            .filter_map(|res| res.ok())
            .flatten()
            .map(|edge| Ok(TraversalVal::Edge(edge)));

        RoTraversalIterator {
            inner: iter,
            storage,
            txn,
        }
    }
}
