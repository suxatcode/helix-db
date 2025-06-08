use super::super::tr_val::TraversalVal;
use crate::helix_engine::{
    bm25::bm25::BM25, graph_core::traversal_iter::RoTraversalIterator, types::GraphError,
};
use crate::helix_storage::lmdb_storage::LmdbStorage;
use crate::helix_storage::Storage;
use std::sync::Arc;

pub struct SearchBM25<'scope, 'inner, S: Storage + ?Sized> {
    txn: &'scope S::RoTxn<'scope>,
    iter: std::vec::IntoIter<(u128, f32)>,
    storage: Arc<S>,
    label: &'inner str,
}

impl<'scope, 'inner, S: Storage + ?Sized> Iterator for SearchBM25<'scope, 'inner, S> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_item = self.iter.next()?;
        match self.storage.get_node(self.txn, &next_item.0) {
            Ok(node) if node.label == self.label => Some(Ok(TraversalVal::Node(node))),
            Ok(_) => self.next(), // Continue searching if label doesn't match
            Err(e) => Some(Err(e)),
        }
    }
}

pub trait SearchBM25Adapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn search_bm25(
        self,
        label: &str,
        query: &str,
        k: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, S> SearchBM25Adapter<'a, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage<RoTxn<'a> = crate::helix_storage::lmdb_storage::LmdbRoTxn<'a>> + 'static,
{
    fn search_bm25(
        self,
        label: &str,
        query: &str,
        k: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let results = if let Some(lmdb_storage) =
            (self.storage.as_ref() as &dyn std::any::Any).downcast_ref::<LmdbStorage>()
        {
            lmdb_storage
                .bm25
                .search(&self.txn, query, k)
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        let iter = SearchBM25 {
            txn: self.txn,
            iter: results.into_iter(),
            storage: Arc::clone(&self.storage),
            label,
        };
        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
