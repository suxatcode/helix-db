use std::sync::Arc;
use std::iter::Once;

use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        types::GraphError,
    },
    helix_storage::Storage,
    protocol::items::Edge,
};

pub struct EFromId<'a, T, S: Storage + ?Sized> {
    iter: Once<Result<TraversalVal, GraphError>>,
    storage: Arc<S>,
    txn: &'a T,
    id: &'a u128,
}

impl<'a, S: Storage + ?Sized> Iterator for EFromId<'a, S::RoTxn<'a>, S> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let edge: Edge = self.storage.get_edge(self.txn, self.id)?;
            Ok(TraversalVal::Edge(edge))
        })
    }
}
pub trait EFromIdAdapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    type OutputIter: Iterator<Item = Result<TraversalVal, GraphError>>;

    /// Returns an iterator containing the edge with the given id.
    ///
    /// Note that the `id` cannot be empty and must be a valid, existing edge id.
    fn e_from_id(self, id: &'a u128) -> Self::OutputIter;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>, S: Storage + ?Sized>
    EFromIdAdapter<'a, S> for RoTraversalIterator<'a, I, S>
{
    type OutputIter = RoTraversalIterator<'a, EFromId<'a, S::RoTxn<'a>, S>, S>;

    #[inline]
    fn e_from_id(self, id: &'a u128) -> Self::OutputIter {
        let e_from_id = EFromId {
            iter: std::iter::once(Ok(TraversalVal::Empty)),
            storage: Arc::clone(&self.storage),
            txn: self.txn,
            id,
        };

        RoTraversalIterator {
            inner: e_from_id,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
