use std::sync::Arc;

use heed3::{RoTxn, RwTxn};

use crate::helix_engine::{storage_core::storage_core::HelixGraphStorage, types::GraphError};

use super::ops::tr_val::TraversalVal;

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

pub trait RoTraversalIteratorAdapter:
    Iterator<Item = Result<TraversalVal, GraphError>> + Sized
{
    fn with<'a>(
        self,
        db: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
    ) -> RoTraversalIterator<'a, Self>;
}

impl<I: Iterator<Item = Result<TraversalVal, GraphError>>> RoTraversalIteratorAdapter for I {
    fn with<'a>(
        self,
        db: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
    ) -> RoTraversalIterator<'a, Self>
    where
        Self: Sized,
    {
        RoTraversalIterator {
            inner: self,
            storage: db,
            txn,
        }
    }
}

pub struct RwTraversalIterator<'a, I> {
    pub inner: I,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'a RwTxn<'a>,
}

// implementing iterator for TraversalIterator
impl<'a, I> Iterator for RwTraversalIterator<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait RwTraversalIteratorAdapter:
    Iterator<Item = Result<TraversalVal, GraphError>> + Sized
{
    fn with<'a>(
        self,
        db: Arc<HelixGraphStorage>,
        txn: &'a RwTxn<'a>,
    ) -> RwTraversalIterator<'a, Self>;
}

impl<I: Iterator<Item = Result<TraversalVal, GraphError>>> RwTraversalIteratorAdapter for I {
    fn with<'a>(
        self,
        db: Arc<HelixGraphStorage>,
        txn: &'a RwTxn<'a>,
    ) -> RwTraversalIterator<'a, Self>
    where
        Self: Sized,
    {
        RwTraversalIterator {
            inner: self,
            storage: db,
            txn,
        }
    }
}
