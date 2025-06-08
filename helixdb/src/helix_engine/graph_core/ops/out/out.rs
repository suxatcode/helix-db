use crate::{
    helix_engine::{
        graph_core::{
            ops::{
                source::add_e::EdgeType,
                tr_val::{Traversable, TraversalVal},
            },
            traversal_iter::RoTraversalIterator,
        },
        types::GraphError,
    },
    helix_storage::Storage,
};
use heed3::{types::Bytes, RoTxn, WithTls};
use std::sync::Arc;

pub struct OutNodesIterator<'a, I, S: Storage + ?Sized> {
    iter: I,
    storage: Arc<S>,
    txn: &'a S::RoTxn<'a>,
    edge_label: &'a str,
}

impl<'a, I, S> Iterator for OutNodesIterator<'a, I, S>
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
                    .get_out_nodes(self.txn, self.edge_label, &start_node.id())?;
            Ok(TraversalVal::NodeArray(nodes))
        })
    }
}

pub trait OutAdapter<'a, S: Storage + ?Sized, I: Iterator<Item = Result<TraversalVal, GraphError>>>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn out(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, S> OutAdapter<'a, S, I> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a,
    S: Storage + ?Sized,
{
    #[inline]
    fn out(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        RoTraversalIterator {
            inner: OutNodesIterator {
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
