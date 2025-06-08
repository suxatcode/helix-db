use std::sync::Arc;

use super::ops::tr_val::TraversalVal;
use crate::helix_storage::{lmdb_storage::LmdbStorage, DbRoTxn, DbRwTxn, Storage};
use crate::helix_engine::types::GraphError;
use itertools::Itertools;

pub struct RoTraversalIterator<'a, I, S: Storage + ?Sized> {
    pub inner: I,
    pub storage: Arc<S>,
    pub txn: &'a S::RoTxn<'a>,
}

// implementing iterator for TraversalIterator
impl<'a, I, S: Storage + ?Sized> Iterator for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>, S: Storage + ?Sized>
    RoTraversalIterator<'a, I, S>
{
    pub fn take_and_collect_to<B: FromIterator<TraversalVal>>(self, n: usize) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .take(n)
            .collect::<B>()
    }

    pub fn collect_to<B: FromIterator<TraversalVal>>(self) -> B {
        self.inner.filter_map(|item| item.ok()).collect::<B>()
    }

    pub fn collect_dedup<B: FromIterator<TraversalVal>>(self) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .unique()
            .collect::<B>()
    }

    pub fn collect_to_obj(self) -> Option<TraversalVal> {
        self.inner.filter_map(|item| item.ok()).take(1).next()
    }
}
pub struct RwTraversalIterator<'scope, 'env, I, S: Storage + ?Sized> {
    pub inner: I,
    pub storage: Arc<S>,
    pub txn: &'scope mut S::RwTxn<'env>,
}

// implementing iterator for TraversalIterator
impl<'scope, 'env, I, S: Storage + ?Sized> Iterator for RwTraversalIterator<'scope, 'env, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
impl<'scope, 'env, I: Iterator, S: Storage + ?Sized> RwTraversalIterator<'scope, 'env, I, S> {
    pub fn new(storage: Arc<S>, txn: &'scope mut S::RwTxn<'env>, inner: I) -> Self {
        Self {
            inner,
            storage,
            txn,
        }
    }

    pub fn collect_to<B: FromIterator<TraversalVal>>(self) -> B
    where
        I: Iterator<Item = Result<TraversalVal, GraphError>>,
    {
        self.inner.filter_map(|item| item.ok()).collect::<B>()
    }

    pub fn collect_to_val(self) -> TraversalVal
    where
        I: Iterator<Item = Result<TraversalVal, GraphError>>,
    {
        match self
            .inner
            .filter_map(|item| item.ok())
            .collect::<Vec<_>>()
            .first()
        {
            Some(val) => val.clone(), // TODO: Remove clone
            None => TraversalVal::Empty,
        }
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
