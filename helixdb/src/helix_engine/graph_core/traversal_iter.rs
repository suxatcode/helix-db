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

pub struct RwTraversalIterator<'a, I> {
    pub inner: I,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'a mut RwTxn<'a>,
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
impl<'a> RwTraversalIterator<'a, std::iter::Once<Result<TraversalVal, GraphError>>> {
    pub fn new(storage: Arc<HelixGraphStorage>, txn: &'a mut RwTxn<'a>) -> Self {
        Self {
            inner: std::iter::once(Ok(TraversalVal::Empty)),
            storage,
            txn,
        }
    }

    // pub fn commit(self) -> Result<I, GraphError> {
    //     self.txn.commit().map_err(|e| GraphError::from(e));
    //     Ok(self.inner)
    // }
}
// pub trait TraversalIteratorMut<'a> {
//     type Inner: Iterator<Item = Result<TraversalVal, GraphError>>;

//     fn next<'b>(
//         &mut self,
//         storage: Arc<HelixGraphStorage>,
//         txn: &'b mut RwTxn<'a>,
//     ) -> Option<Result<TraversalVal, GraphError>>;

// }
