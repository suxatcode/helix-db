use std::collections::HashMap;

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        types::GraphError,
    },
    helix_storage::Storage,
    protocol::value::Value,
};

use super::super::tr_val::TraversalVal;

pub struct Update<I> {
    iter: I,
}

impl<I> Iterator for Update<I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait UpdateAdapter<'scope, 'env, S: Storage + ?Sized>: Iterator {
    fn update(
        self,
        props: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'scope, 'env, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'scope, 'env, I, S> UpdateAdapter<'scope, 'env, S> for RwTraversalIterator<'scope, 'env, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    fn update(
        mut self,
        props: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'scope, 'env, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    {
        let storage = self.storage.clone();

        let capacity = match self.inner.size_hint() {
            (_, Some(upper)) => upper,
            (lower, None) => lower,
        };
        let mut vec = Vec::with_capacity(capacity);

        for item in self.inner {
            match item {
                Ok(TraversalVal::Node(node)) => {
                    match storage.update_node(self.txn, &node.id, props.as_ref()) {
                        Ok(updated_node) => vec.push(Ok(TraversalVal::Node(updated_node))),
                        Err(e) => vec.push(Err(e)),
                    }
                }
                Ok(TraversalVal::Edge(edge)) => {
                    match storage.update_edge(self.txn, &edge.id, props.as_ref()) {
                        Ok(updated_edge) => vec.push(Ok(TraversalVal::Edge(updated_edge))),
                        Err(e) => vec.push(Err(e)),
                    }
                }
                _ => vec.push(Err(GraphError::new("Unsupported value type".to_string()))),
            }
        }
        RwTraversalIterator {
            inner: Update {
                iter: vec.into_iter(),
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
