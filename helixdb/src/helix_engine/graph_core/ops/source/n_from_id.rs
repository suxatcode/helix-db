use std::sync::Arc;
use std::iter::Once;

use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        types::GraphError,
    },
    helix_storage::Storage,
    protocol::items::Node,
};

pub struct NFromId<'a, T, S: Storage + ?Sized> {
    iter: Once<Result<TraversalVal, GraphError>>,
    storage: Arc<S>,
    txn: &'a T,
    id: u128,
}

impl<'a, S: Storage + ?Sized> Iterator for NFromId<'a, S::RoTxn<'a>, S> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let node: Node = self.storage.get_node(self.txn, &self.id)?;
            Ok(TraversalVal::Node(node))
        })
    }
}

pub trait NFromIdAdapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    type OutputIter: Iterator<Item = Result<TraversalVal, GraphError>>;

    /// Returns an iterator containing the node with the given id.
    ///
    /// Note that the `id` cannot be empty and must be a valid, existing node id.
    fn n_from_id(self, id: &u128) -> Self::OutputIter;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>, S: Storage + ?Sized>
    NFromIdAdapter<'a, S> for RoTraversalIterator<'a, I, S>
{
    type OutputIter = RoTraversalIterator<'a, NFromId<'a, S::RoTxn<'a>, S>, S>;

    #[inline]
    fn n_from_id(self, id: &u128) -> Self::OutputIter {
        let n_from_id = NFromId {
            iter: std::iter::once(Ok(TraversalVal::Empty)),
            storage: Arc::clone(&self.storage),
            txn: self.txn,
            id: *id,
        };

        RoTraversalIterator {
            inner: n_from_id,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
