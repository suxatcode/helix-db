use std::sync::Arc;

use heed3::{RoTxn, RwTxn};

use super::ops::tr_val::TraversalVal;
use crate::helix_engine::{
    graph_core::ops::tr_val::Traversable, storage_core::storage_core::HelixGraphStorage,
    types::GraphError,
};
use itertools::Itertools;

pub struct RoTraversalIterator<'a, I> {
    pub inner: I,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'a RoTxn<'a>,
}

// implementing iterator for TraversalIterator
impl<'a, I> Iterator for RoTraversalIterator<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> RoTraversalIterator<'a, I> {
    pub fn collect_to<B: FromIterator<TraversalVal>>(self) -> B {
        self.inner.filter_map(|item| item.ok()).collect::<B>()
    }

    pub fn collect_dedup<B: FromIterator<TraversalVal>>(self) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .unique()
            .collect::<B>()
    }
}
pub struct RwTraversalIterator<'a, 'b, I> {
    pub inner: I,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'b mut RwTxn<'a>,
}

// implementing iterator for TraversalIterator
impl<'a, 'b, I> Iterator for RwTraversalIterator<'a, 'b, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
impl<'a, 'b> RwTraversalIterator<'a, 'b, std::iter::Once<Result<TraversalVal, GraphError>>> {
    pub fn new(storage: Arc<HelixGraphStorage>, txn: &'b mut RwTxn<'a>) -> Self {
        Self {
            inner: std::iter::once(Ok(TraversalVal::Empty)),
            storage,
            txn,
        }
    }

    pub fn collect_to<
        I: Iterator<Item = Result<TraversalVal, GraphError>>,
        B: FromIterator<TraversalVal>,
    >(
        self,
    ) -> B {
        self.inner.filter_map(|item| item.ok()).collect::<B>()
    }
}
// pub trait TraversalIteratorMut<'a> {
//     type Inner: Iterator<Item = Result<TraversalVal, GraphError>>;

//     fn next<'b>(
//         &mut self,
//         storage: Arc<HelixGraphStorage>,
//         txn: &'b mut RwTxn<'a>,
//     ) -> Option<Result<TraversalVal, GraphError>>;

// }
