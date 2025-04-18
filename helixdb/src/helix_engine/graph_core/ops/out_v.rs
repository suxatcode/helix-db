use super::tr_val::TraversalVal;
use crate::helix_engine::storage_core::{
    storage_core::HelixGraphStorage, storage_methods::StorageMethods,
};
use heed3::RoTxn;
use std::sync::Arc;

pub struct OutVIterator<'a, I> {
    iter: I,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
}

// implementing iterator for OutIterator
impl<'a, I> Iterator for OutVIterator<'a, I>
where
    I: Iterator<Item = TraversalVal>,
{
    type Item = TraversalVal;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => match item {
                TraversalVal::Edge(item) => Some(TraversalVal::Node(
                    self.storage.get_node(self.txn, &item.to_node).unwrap(),
                )), // TODO: handle unwrap
                _ => return None,
            },
            None => None,
        }
    }
}
pub trait OutVAdapter: Iterator {
    fn out_v<'a>(self, db: Arc<HelixGraphStorage>, txn: &'a RoTxn<'a>) -> OutVIterator<'a, Self>
    where
        Self: Sized + Iterator<Item = TraversalVal> + 'a,
        Self::Item: Send,
    {
        OutVIterator {
            iter: self,
            storage: db,
            txn,
        }
    }
}

impl<T: ?Sized> OutVAdapter for T where T: Iterator {}
