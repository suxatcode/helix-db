use std::sync::Arc;

use heed3::{RoTxn, RwTxn};

use crate::{
    helix_engine::{
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

pub struct FilterMut<'a, I, F> {
    iter: I,
    txn: &'a mut RwTxn<'a>,
    f: F,
}

// implementing iterator for filter ref
impl<'a, I, F> Iterator for FilterMut<'a, I, F>
where
    I: Iterator,
    I::Item: Filterable<'a>,
    F: Fn(&mut I::Item, &mut RwTxn) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(mut item) => match (self.f)(&mut item, &mut self.txn) {
                true => Some(item),
                false => None,
            },
            None => None,
        }
    }
}

pub trait FilterMutAdapter: Iterator {
    /// FilterMut filters the iterator by taking a mutable
    /// reference to each item and a transaction.
    fn filter_mut<'a, F>(self, txn: &'a mut RwTxn<'a>, f: F) -> FilterMut<'a, Self, F>
    where
        Self: Sized + Iterator,
        Self::Item: Send,
        F: Fn(&mut Self::Item, &RwTxn) -> bool,
    {
        FilterMut { iter: self, txn, f }
    }
}

impl<T: ?Sized> FilterMutAdapter for T where T: Iterator {}
