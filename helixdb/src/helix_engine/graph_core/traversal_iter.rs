use std::sync::Arc;

use heed3::RoTxn;

use crate::helix_engine::storage_core::storage_core::HelixGraphStorage;

pub struct TraversalIterator<'a, I, F> {
    iter: I,
    f: F,
    storage: Arc<HelixGraphStorage>,
    txn: RoTxn<'a>,
}

// implementing iterator for TraversalIterator
impl<'a, I, F, B> Iterator for TraversalIterator<'a, I, F>
where
    I: Iterator,
    F: FnMut(I::Item) -> B,
{
    type Item = B;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|item| (self.f)(item))
    }
}

pub trait TraversalIteratorAdapter: Iterator {
    fn apply_fn<B, F>(
        self,
        f: F,
        db: Arc<HelixGraphStorage>,
        txn: RoTxn<'_>,
    ) -> TraversalIterator<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> B,
    {
        TraversalIterator {
            iter: self,
            f,
            storage: db,
            txn,
        }
    }
}

impl<T: ?Sized> TraversalIteratorAdapter for T where T: Iterator {}
