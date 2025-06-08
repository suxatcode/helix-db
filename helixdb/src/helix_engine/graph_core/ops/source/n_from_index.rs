use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        types::GraphError,
    },
    helix_storage::Storage,
    protocol::{items::Node, value::Value},
};
use serde::Serialize;
use std::sync::Arc;

pub struct NFromIndex<S: Storage + ?Sized> {
    nodes: Vec<Node>,
    storage: Arc<S>,
}

impl<S: Storage + ?Sized> Iterator for NFromIndex<S> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.nodes.pop().map(|n| Ok(TraversalVal::Node(n)))
    }
}

pub trait NFromIndexAdapter<'a, K, S>: Iterator<Item = Result<TraversalVal, GraphError>>
where
    K: Into<Value> + Serialize,
    S: Storage + ?Sized,
{
    fn n_from_index(
        self,
        index: &'a str,
        key: &'a K,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, K, S> NFromIndexAdapter<'a, K, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    K: Into<Value> + Serialize + 'a,
    S: Storage + ?Sized,
{
    #[inline]
    fn n_from_index(
        self,
        index: &'a str,
        key: &'a K,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let storage = self.storage.clone();
        let txn = self.txn;
        let nodes = storage
            .node_from_index(txn, index, key)
            .unwrap_or_default();

        let iter = NFromIndex {
            nodes,
            storage: Arc::clone(&storage),
        };

        RoTraversalIterator {
            inner: iter,
            storage,
            txn,
        }
    }
}
