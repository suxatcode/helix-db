use super::super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        types::{GraphError, VectorError},
        vector_core::{hnsw::HNSW, vector::HVector},
    },
    helix_storage::{lmdb_storage::LmdbStorage, Storage},
    protocol::value::Value,
};
use std::sync::Arc;

pub trait InsertVAdapter<'a, 'b, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn insert_v(
        self,
        vec: &Vec<f64>,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;

    fn insert_vs(
        self,
        vecs: &Vec<Vec<f64>>,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, 'b, I, S> InsertVAdapter<'a, 'b, S> for RwTraversalIterator<'a, 'b, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage<RwTxn<'b> = crate::helix_storage::lmdb_storage::LmdbRwTxn<'b>> + 'static,
{
    fn insert_v(
        mut self,
        query: &Vec<f64>,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let result = if let Some(lmdb_storage) =
            (self.storage.as_ref() as &dyn std::any::Any).downcast_ref::<LmdbStorage>()
        {
            lmdb_storage
                .vectors
                .insert(self.txn, &query, fields)
                .map(TraversalVal::Vector)
                .map_err(GraphError::from)
        } else {
            Err(GraphError::from(VectorError::VectorCoreError(
                "Vector insert is only supported on LmdbStorage".to_string(),
            )))
        };

        RwTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            txn: self.txn,
        }
    }

    fn insert_vs(
        mut self,
        vecs: &Vec<Vec<f64>>,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let mut results = Vec::with_capacity(vecs.len());

        if let Some(lmdb_storage) =
            (self.storage.as_ref() as &dyn std::any::Any).downcast_ref::<LmdbStorage>()
        {
            for vec in vecs {
                let result = lmdb_storage
                    .vectors
                    .insert(self.txn, vec, fields.clone())
                    .map(TraversalVal::Vector)
                    .map_err(GraphError::from);
                results.push(result);
            }
        } else {
            results.push(Err(GraphError::from(VectorError::VectorCoreError(
                "Vector insert is only supported on LmdbStorage".to_string(),
            ))));
        }

        RwTraversalIterator {
            inner: results.into_iter(),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
