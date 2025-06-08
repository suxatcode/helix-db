use super::super::tr_val::TraversalVal;
use crate::helix_engine::{
    graph_core::traversal_iter::RoTraversalIterator,
    types::{GraphError, VectorError},
    vector_core::{hnsw::HNSW, vector::HVector},
};
use crate::helix_storage::{lmdb_storage::LmdbStorage, Storage};
use heed3::RoTxn;
use std::iter::once;

pub struct SearchV<I: Iterator<Item = Result<TraversalVal, GraphError>>> {
    iter: I,
}

impl<I: Iterator<Item = Result<TraversalVal, GraphError>>> Iterator for SearchV<I> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait SearchVAdapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn search_v<F>(
        self,
        query: &Vec<f64>,
        k: usize,
        filter: Option<&[F]>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        F: Fn(&HVector, &heed3::RoTxn<'a>) -> bool;
}

impl<'a, I, S> SearchVAdapter<'a, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a,
    S: Storage<RoTxn<'a> = crate::helix_storage::lmdb_storage::LmdbRoTxn<'a>> + 'static,
{
    fn search_v<F>(
        self,
        query: &Vec<f64>,
        k: usize,
        filter: Option<&[F]>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        F: Fn(&HVector, &heed3::RoTxn<'a>) -> bool,
    {
        let vectors = if let Some(lmdb_storage) =
            (self.storage.as_ref() as &dyn std::any::Any).downcast_ref::<LmdbStorage>()
        {
            lmdb_storage
                .vectors
                .search(self.txn, &query, k, filter, false)
        } else {
            Err(VectorError::VectorCoreError(
                "Vector search is only supported on LmdbStorage".to_string(),
            ))
        };

        let iter = match vectors {
            Ok(vectors) => vectors
                .into_iter()
                .map(|vector| Ok(TraversalVal::Vector(vector)))
                .collect::<Vec<_>>()
                .into_iter(),
            Err(e) => once(Err(GraphError::from(e)))
                .collect::<Vec<_>>()
                .into_iter(),
        };

        let iter = SearchV { iter };

        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
