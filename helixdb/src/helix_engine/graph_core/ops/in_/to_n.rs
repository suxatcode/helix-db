use crate::helix_engine::{
    graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
    types::GraphError,
};
use crate::helix_storage::Storage;
use std::sync::Arc;

pub struct ToNIterator<'a, I, T, S: Storage + ?Sized> {
    iter: I,
    storage: Arc<S>,
    txn: &'a T,
}

impl<'a, I, S: Storage + ?Sized> Iterator for ToNIterator<'a, I, S::RoTxn<'a>, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|item| match item {
            Ok(TraversalVal::Edge(edge)) => {
                Some(self.storage.get_node(self.txn, &edge.to_node).map(TraversalVal::Node))
            }
            _ => None,
        })
    }
}

pub trait ToNAdapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn to_n(
        self,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, S> ToNAdapter<'a, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    #[inline(always)]
    fn to_n(
        self,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let iter = ToNIterator {
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
