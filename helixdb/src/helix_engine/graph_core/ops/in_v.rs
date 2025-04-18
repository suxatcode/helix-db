use super::tr_val::TraversalVal;
use crate::helix_engine::storage_core::{
    storage_core::HelixGraphStorage, storage_methods::StorageMethods,
};
use heed3::RoTxn;
use std::sync::Arc;

pub struct InVIterator<'a, I> {
    iter: I,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
}

// implementing iterator for InIterator
impl<'a, I> Iterator for InVIterator<'a, I>
where
    I: Iterator<Item = TraversalVal>,
{
    type Item = TraversalVal;

    /// Returns the next ingoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => match item {
                TraversalVal::Edge(item) => Some(TraversalVal::Node(
                    self.storage.get_node(self.txn, &item.from_node).unwrap(),
                )), // TODO: handle unwrap
                _ => return None,
            },
            None => None,
        }
    }
}
pub trait InVAdapter: Iterator {
    fn in_v<'a>(self, db: Arc<HelixGraphStorage>, txn: &'a RoTxn<'a>) -> InVIterator<'a, Self>
    where
        Self: Sized + Iterator<Item = TraversalVal> + 'a,
        Self::Item: Send,
    {
        InVIterator {
            iter: self,
            storage: db,
            txn,
        }
    }
}

impl<T: ?Sized> InVAdapter for T where T: Iterator {}
