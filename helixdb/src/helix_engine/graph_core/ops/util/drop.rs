use std::sync::Arc;

use heed3::RwTxn;

use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RwTraversalIterator},
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::filterable::{Filterable, FilterableType},
};

pub struct Drop<'a, I> {
    iter: I,
    storage: Arc<HelixGraphStorage>,
    txn: &'a mut RwTxn<'a>,
}

// implementing iterator for Drop
impl<'a, I> Iterator for Drop<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = ();

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(item)) => {
                match item {
                    TraversalVal::Node(node) => {
                        self.storage.drop_node(&mut self.txn, &node.id).unwrap();
                    }
                    TraversalVal::Edge(edge) => {
                        self.storage.drop_edge(&mut self.txn, &edge.id).unwrap();
                    }
                    // FilterableType::Vector => self.storage.drop_vector(&self.txn, &item.id());
                    _ => {
                        return None;
                    }
                }
                Some(())
            }
            Some(Err(e)) => {
                None
            }
            None => None,
        }
    }
}

pub trait DropAdapter<'a, 'b>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn drop(self) -> Drop<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> DropAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
where
    'b: 'a,
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    fn drop(self) -> Drop<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        Drop {
            iter: self.inner,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
