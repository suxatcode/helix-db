use crate::helix_engine::{graph_core::ops::tr_val::TraversalVal, types::GraphError};
use crate::helix_storage::Storage;

pub struct FilterMut<'a, 'env, I, F, S: Storage + ?Sized> {
    iter: I,
    txn: &'a mut S::RwTxn<'env>,
    f: F,
}

impl<'a, 'env, I, F, S: Storage + ?Sized> Iterator for FilterMut<'a, 'env, I, F, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(&I::Item, &mut S::RwTxn<'env>) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.iter.next() {
            if (self.f)(&item, self.txn) {
                return Some(item);
            }
        }
        None
    }
}