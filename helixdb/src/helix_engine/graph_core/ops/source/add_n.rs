use super::super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        types::GraphError,
    },
    helix_storage::Storage,
    protocol::{
        value::Value,
    },
};

pub struct AddNIterator {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for AddNIterator {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait AddNAdapter<'a, 'b, S: Storage + ?Sized>: Iterator<Item = Result<TraversalVal, GraphError>> {
    fn add_n(
        self,
        label: &'a str,
        properties: Option<Vec<(String, Value)>>,
        secondary_indices: Option<&'a [&str]>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, 'b, I, S> AddNAdapter<'a, 'b, S> for RwTraversalIterator<'a, 'b, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    fn add_n(
        mut self,
        label: &'a str,
        properties: Option<Vec<(String, Value)>>,
        secondary_indices: Option<&'a [&str]>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let result = self.storage.add_node(
            self.txn,
            label,
            properties,
            secondary_indices,
        );

        let result = match result {
            Ok(node) => Ok(TraversalVal::Node(node)),
            Err(e) => Err(e),
        };

        RwTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
