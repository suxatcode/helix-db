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

use super::tr_val::TraversalVal;

pub struct FilterRef<'a, I, F> {
    iter: I,
    txn: &'a RoTxn<'a>,
    f: F,
}

// implementing iterator for filter ref
impl<'a, I, F> Iterator for FilterRef<'a, I, F>
where
    I: Iterator<Item = TraversalVal>,
    F: Fn(&I::Item, &RoTxn) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => match (self.f)(&item, &self.txn) {
                true => Some(item),
                false => None,
            },
            None => None,
        }
    }
}

pub trait FilterRefAdapter: Iterator {
    /// FilterRef filters the iterator by taking a reference
    /// to each item and a transaction.
    fn filter_ref<'a, F>(self, txn: &'a RoTxn<'a>, f: F) -> FilterRef<'a, Self, F>
    where
        Self: Sized + Iterator,
        Self::Item: Send,
        F: Fn(&Self::Item, &RoTxn) -> bool,
    {
        FilterRef { iter: self, txn, f }
    }
}

impl<T: ?Sized> FilterRefAdapter for T where T: Iterator {}
