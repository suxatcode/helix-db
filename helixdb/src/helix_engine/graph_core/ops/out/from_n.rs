use crate::helix_engine::{
    graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
    types::GraphError,
};
use crate::helix_storage::Storage;
use std::sync::Arc;

pub struct FromNIterator<'a, I, T, S: Storage + ?Sized> {
    iter: I,
    storage: Arc<S>,
    txn: &'a T,
}

impl<'a, I, S: Storage + ?Sized> Iterator for FromNIterator<'a, I, S::RoTxn<'a>, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|item| match item {
            Ok(TraversalVal::Edge(edge)) => Some(
                self.storage
                    .get_node(self.txn, &edge.from_node)
                    .map(TraversalVal::Node),
            ),
            _ => None,
        })
    }
}

pub trait FromNAdapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn from_n(
        self,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, S> FromNAdapter<'a, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a,
    S: Storage + ?Sized,
{
    #[inline(always)]
    fn from_n(
        self,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let iter = FromNIterator {
            iter: self.inner,
            storage: Arc::clone(&self.storage),
            txn: self.txn,
        };
        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
