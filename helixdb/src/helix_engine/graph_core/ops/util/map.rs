use crate::helix_engine::{
    graph_core::traversal_iter::{RoTraversalIterator, RwTraversalIterator},
    types::GraphError,
};
use crate::helix_storage::Storage;

use super::super::tr_val::TraversalVal;

pub struct Map<'a, I, F, S: Storage + ?Sized> {
    iter: I,
    txn: &'a S::RoTxn<'a>,
    f: F,
}

// implementing iterator for filter ref
impl<'a, I, F, S: Storage + ?Sized> Iterator for Map<'a, I, F, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(TraversalVal, &S::RoTxn<'a>) -> Result<TraversalVal, GraphError>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.iter.next() {
            return match item {
                Ok(item) => Some((self.f)(item, self.txn)),
                Err(e) => Some(Err(e)),
            };
        }
        None
    }
}

pub trait MapAdapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    /// MapTraversal maps the iterator by taking a reference
    /// to each item and a transaction.
    ///
    /// # Arguments
    ///
    /// * `f` - A function to map the iterator
    ///
    /// # Example
    ///
    /// ```rust
    /// let traversal = G::new(storage, &txn).map_traversal(|item, txn| {
    ///     Ok(item)
    /// });
    /// ```
    fn map_traversal<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        F: FnMut(TraversalVal, &S::RoTxn<'a>) -> Result<TraversalVal, GraphError>;
}

impl<'a, I, S> MapAdapter<'a, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    #[inline]
    fn map_traversal<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        F: FnMut(TraversalVal, &S::RoTxn<'a>) -> Result<TraversalVal, GraphError>,
    {
        RoTraversalIterator {
            inner: Map::<I, F, S> {
                iter: self.inner,
                txn: self.txn,
                f,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}

pub struct MapMut<I, F> {
    iter: I,
    f: F,
}
impl<I, F> Iterator for MapMut<I, F>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: Fn(I::Item) -> Result<TraversalVal, GraphError>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.iter.next() {
            if let Ok(item) = (self.f)(item) {
                return Some(Ok(item));
            }
        }
        None
    }
}
pub trait MapAdapterMut<'scope, 'env, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    /// MapTraversalMut maps the iterator by taking a mutable
    /// reference to each item and a transaction.
    ///
    /// # Arguments
    ///
    /// * `f` - A function to map the iterator
    fn map_traversal_mut<F>(
        self,
        f: F,
    ) -> RwTraversalIterator<'scope, 'env, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        F: Fn(Result<TraversalVal, GraphError>) -> Result<TraversalVal, GraphError>;
}

impl<'scope, 'env, I, S> MapAdapterMut<'scope, 'env, S> for RwTraversalIterator<'scope, 'env, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    #[inline]
    fn map_traversal_mut<F>(
        self,
        f: F,
    ) -> RwTraversalIterator<'scope, 'env, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        F: Fn(I::Item) -> Result<TraversalVal, GraphError>,
    {
        RwTraversalIterator {
            inner: MapMut {
                iter: self.inner,
                f,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
