use std::sync::Arc;

use heed3::RwTxn;

use crate::{
    helix_engine::{
        graph_core::{
            ops::tr_val::TraversalVal,
            traversal_iter::{RoTraversalIterator, RwTraversalIterator},
        },
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::filterable::{Filterable, FilterableType},
};

pub struct Drop<I> {
    pub iter: I,
}

// implementing iterator for Drop
impl<I> Iterator for Drop<I>
where
    I: Iterator<Item = Result<(), GraphError>>,
{
    type Item = Result<(), GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait DropAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn drop(self) -> Result<(), GraphError>;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> DropAdapter<'a>
    for RwTraversalIterator<'a, 'b, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    fn drop(mut self) -> Result<(), GraphError> {
        // TODO: make sure this isnt stupid as running full loop here will
        // immediately consume drop everything instead of iterating

        let txn = self.txn;
        let storage = Arc::clone(&self.storage);

        self.inner.try_for_each(|item| -> Result<(), GraphError> {
            match item {
                Ok(item) => {
                    match item {
                        TraversalVal::Node(node) => match storage.drop_node(txn, &node.id) {
                            Ok(_) => Ok(()),
                            Err(e) => return Err(e),
                        },
                        TraversalVal::Edge(edge) => match storage.drop_edge(txn, &edge.id) {
                            Ok(_) => Ok(()),
                            Err(e) => return Err(e),
                        },
                        // FilterableType::Vector => self.storage.drop_vector(&self.txn, &item.id());
                        _ => {
                            return Err(GraphError::ConversionError(format!(
                                "Incorrect Type: {:?}",
                                item
                            )));
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        })
    }
}

impl<'a> Drop<Vec<Result<TraversalVal, GraphError>>> {
    pub fn drop_traversal(
        iter: Vec<Result<TraversalVal, GraphError>>,
        storage: Arc<HelixGraphStorage>,
        txn: &mut RwTxn,
    ) -> Result<(), GraphError> {
        iter.into_iter()
            .try_for_each(|item| -> Result<(), GraphError> {
                match item {
                    Ok(item) => {
                        match item {
                            TraversalVal::Node(node) => match storage.drop_node(txn, &node.id) {
                                Ok(_) => Ok(()),
                                Err(e) => return Err(e),
                            },
                            TraversalVal::Edge(edge) => match storage.drop_edge(txn, &edge.id) {
                                Ok(_) => Ok(()),
                                Err(e) => return Err(e),
                            },
                            // FilterableType::Vector => self.storage.drop_vector(&self.txn, &item.id());
                            _ => {
                                return Err(GraphError::ConversionError(format!(
                                    "Incorrect Type: {:?}",
                                    item
                                )));
                            }
                        }
                    }
                    Err(e) => return Err(e),
                }
            })
    }
}
