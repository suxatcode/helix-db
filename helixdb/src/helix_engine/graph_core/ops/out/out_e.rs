use crate::{
    helix_engine::{
        graph_core::{
            ops::tr_val::{Traversable, TraversalVal},
            traversal_iter::RoTraversalIterator,
        },
        types::GraphError,
    },
    helix_storage::Storage,
};
use std::sync::Arc;

pub struct OutEdgesIterator<'a, I, S: Storage + ?Sized> {
    iter: I,
    storage: Arc<S>,
    txn: &'a S::RoTxn<'a>,
    edge_label: &'a str,
}

impl<'a, I, S> Iterator for OutEdgesIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|item| {
            let start_node = item?;
            let edges = self
                .storage
                .get_out_edges(self.txn, self.edge_label, &start_node.id())?;
            Ok(TraversalVal::EdgeArray(edges))
        })
    }
}

pub trait OutEdgesAdapter<'a, S: Storage + ?Sized, I: Iterator<Item = Result<TraversalVal, GraphError>>>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn out_e(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, S> OutEdgesAdapter<'a, S, I> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a,
    S: Storage + ?Sized,
{
    #[inline]
    fn out_e(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        RoTraversalIterator {
            inner: OutEdgesIterator {
                iter: self.inner,
                storage: self.storage.clone(),
                txn: self.txn,
                edge_label,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
