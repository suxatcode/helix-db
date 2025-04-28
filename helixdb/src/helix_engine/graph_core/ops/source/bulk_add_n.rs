use heed3::PutFlags;
use std::{collections::HashMap, time::Instant};
use uuid::Uuid;

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        storage_core::storage_core::HelixGraphStorage, types::GraphError,
    },
    protocol::{
        filterable::Filterable,
        items::{Edge, Node},
        value::Value,
    },
};

use super::super::tr_val::TraversalVal;

pub struct BulkAddN {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for BulkAddN {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait BulkAddNAdapter<'a, 'b>:
    Iterator<Item = Result<TraversalVal, GraphError>> + Sized
{
    fn bulk_add_n(
        self,
        nodes: &mut [Node],
        secondary_indices: Option<&[String]>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>>> BulkAddNAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn bulk_add_n(
        self,
        nodes: &mut [Node],
        secondary_indices: Option<&[String]>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);
        for node in nodes {
            let secondary_indices = secondary_indices.unwrap_or(&[]).to_vec();
            // insert node
            match bincode::serialize(&node) {
                Ok(bytes) => {
                    if let Err(e) = self.storage.nodes_db.put_with_flags(
                        self.txn,
                        PutFlags::APPEND,
                        &HelixGraphStorage::node_key(&node.id),
                        &bytes,
                    ) {
                        result = Err(GraphError::from(e));
                    }
                }
                Err(e) => result = Err(GraphError::from(e)),
            }

            // insert label
            match self.storage.node_labels_db.put_with_flags(
                self.txn,
                PutFlags::APPEND,
                &HelixGraphStorage::node_label_key(&node.label, Some(&node.id)),
                &(),
            ) {
                Ok(_) => {}
                Err(e) => result = Err(GraphError::from(e)),
            }

            for index in &secondary_indices {
                match self.storage.secondary_indices.get(index.as_str()) {
                    Some(db) => {
                        let key = match node.check_property(&index) {
                            Some(value) => value,
                            None => {
                                result = Err(GraphError::New(format!(
                                    "Secondary Index {} not found",
                                    index
                                )));
                                continue;
                            }
                        };
                        match bincode::serialize(&key) {
                            Ok(serialized) => {
                                if let Err(e) = db.put_with_flags(
                                    self.txn,
                                    PutFlags::APPEND,
                                    &serialized,
                                    &node.id.to_be_bytes(),
                                ) {
                                    result = Err(GraphError::from(e));
                                }
                            }
                            Err(e) => result = Err(GraphError::from(e)),
                        }
                    }
                    None => {
                        result = Err(GraphError::New(format!(
                            "Secondary Index {} not found",
                            index
                        )));
                    }
                }
            }
        }
        RwTraversalIterator {
            inner: std::iter::once(result), // TODO: change to support adding multiple edges
            storage: self.storage,
            txn: self.txn,
        }
    }
}
