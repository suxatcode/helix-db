use crate::helix_engine::{
    graph_core::traversal_iter::RoTraversalIterator, types::GraphError,
};
use crate::helix_storage::Storage;

use super::super::tr_val::TraversalVal;

pub struct FilterRef<'a, I, F, S: Storage + ?Sized> {
    iter: I,
    txn: &'a S::RoTxn<'a>,
    f: F,
}

impl<'a, I, F, S: Storage + ?Sized> Iterator for FilterRef<'a, I, F, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: Fn(&I::Item, &S::RoTxn<'a>) -> Result<bool, GraphError>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.iter.next() {
            match (self.f)(&item, self.txn) {
                Ok(true) => return Some(item),
                Ok(false) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

pub trait FilterRefAdapter<'a, S: Storage + ?Sized>: Iterator {
    fn filter_ref<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        F: Fn(&Result<TraversalVal, GraphError>, &S::RoTxn<'a>) -> Result<bool, GraphError>;
}

impl<'a, I, S> FilterRefAdapter<'a, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    #[inline]
    fn filter_ref<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        F: Fn(&Result<TraversalVal, GraphError>, &S::RoTxn<'a>) -> Result<bool, GraphError>,
    {
        RoTraversalIterator {
            inner: FilterRef::<I, F, S> {
                iter: self.inner,
                txn: self.txn,
                f,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
