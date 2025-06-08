use crate::helix_engine::{
    graph_core::ops::tr_val::TraversalVal,
    types::GraphError,
};
use crate::helix_storage::Storage;
use std::sync::Arc;

pub struct Drop<I> {
    pub iter: I,
}

impl<I> Drop<I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    pub fn drop_traversal<'a, S: Storage + ?Sized>(
        iter: I,
        storage: Arc<S>,
        txn: &mut S::RwTxn<'a>,
    ) -> Result<(), GraphError> {
        iter.try_for_each(|item| -> Result<(), GraphError> {
            match item? {
                TraversalVal::Node(node) => storage.drop_node(txn, &node.id),
                TraversalVal::Edge(edge) => storage.drop_edge(txn, &edge.id),
                _ => Err(GraphError::WrongTraversalValue),
            }
        })
    }
}
