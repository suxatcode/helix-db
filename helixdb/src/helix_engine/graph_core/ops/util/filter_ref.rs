use crate::helix_engine::{graph_core::traversal_iter::RoTraversalIterator, types::GraphError};

use super::super::tr_val::TraversalVal;
use heed3::RoTxn;

pub struct FilterRef<'a, I, F> {
    iter: I,
    txn: &'a RoTxn<'a>,
    f: F,
}

// implementing iterator for filter ref
impl<'a, I, F> Iterator for FilterRef<'a, I, F>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: Fn(&I::Item, &RoTxn) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.iter.next() {
            if (self.f)(&item, &self.txn) {
                return Some(item);
            }
        }
        None
    }
}

pub trait FilterRefAdapter<'a>: Iterator + Sized {
    /// FilterRef filters the iterator by taking a reference
    /// to each item and a transaction.
    fn filter_ref<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(&Result<TraversalVal, GraphError>, &RoTxn) -> bool;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> FilterRefAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn filter_ref<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(&Result<TraversalVal, GraphError>, &RoTxn) -> bool,
    {
        RoTraversalIterator {
            inner: FilterRef {
                iter: self.inner,
                txn: self.txn,
                f,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
