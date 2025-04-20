use crate::{
    helix_engine::{
        graph_core::{
            ops::tr_val::TraversalVal,
            traversal_iter::{RoTraversalIterator},
        },
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::items::{Edge, Node},
};
use heed3::{RoTxn, RwTxn};
use std::sync::Arc;

pub struct InVIterator<'a, I, T> {
    iter: I,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
}

// implementing iterator for OutIterator
impl<'a, I> Iterator for InVIterator<'a, I, RoTxn<'a>>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => match item {
                Ok(TraversalVal::Edge(item)) => Some(Ok(TraversalVal::Node(
                    self.storage.get_node(self.txn, &item.from_node).unwrap(),
                ))), // TODO: handle unwrap
                _ => return None,
            },
            None => None,
        }
    }
}

impl<'a, I> Iterator for InVIterator<'a, I, RwTxn<'a>>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => match item {
                Ok(TraversalVal::Edge(item)) => Some(Ok(TraversalVal::Node(
                    self.storage.get_node(self.txn, &item.to_node).unwrap(),
                ))), // TODO: handle unwrap
                _ => return None,
            },
            None => None,
        }
    }
}
pub trait InVAdapter<'a, T>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn in_v(self, db: Arc<HelixGraphStorage>, txn: &'a T) -> InVIterator<'a, Self, T>
    where
        Self: Sized + Iterator<Item = Result<TraversalVal, GraphError>> + 'a,
        Self::Item: Send;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> InVAdapter<'a, RoTxn<'a>>
    for RoTraversalIterator<'a, I>
{
    fn in_v(
        self,
        db: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
    ) -> InVIterator<'a, Self, RoTxn<'a>> {
        InVIterator {
            iter: self,
            storage: db,
            txn,
        }
    }
}

// impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> InVAdapter<'a, RwTxn<'a>>
//     for RwTraversalIterator<'a, I>
// {
//     fn in_v(
//         self,
//         db: Arc<HelixGraphStorage>,
//         txn: &'a RwTxn<'a>,
//     ) -> InVIterator<'a, Self, RwTxn<'a>> {
//         InVIterator {
//             iter: self,
//             storage: db,
//             txn,
//         }
//     }
// }
