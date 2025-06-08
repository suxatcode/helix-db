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
use heed3::{types::Bytes, RoTxn};
use std::sync::Arc;

pub struct InNodesIterator<'a, I, S: Storage + ?Sized> {
    iter: I,
    storage: Arc<S>,
    txn: &'a S::RoTxn<'a>,
    edge_label: &'a str,
}

impl<'a, I, S> Iterator for InNodesIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|item| {
            let start_node = item?;
            let nodes =
                self.storage
                    .get_in_nodes(self.txn, self.edge_label, &start_node.id())?;
            Ok(TraversalVal::NodeArray(nodes))
        })
    }
}

pub trait InAdapter<'a, S: Storage + ?Sized, I: Iterator<Item = Result<TraversalVal, GraphError>>>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn in_(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, S> InAdapter<'a, S, I> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a,
    S: Storage + ?Sized,
{
    #[inline]
    fn in_(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        RoTraversalIterator {
            inner: InNodesIterator {
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
