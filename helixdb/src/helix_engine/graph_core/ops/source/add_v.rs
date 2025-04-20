use std::{collections::HashMap, iter::Once, sync::Arc};

use heed3::{RoTxn, RwTxn};
use uuid::Uuid;

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        storage_core::storage_core::HelixGraphStorage, types::GraphError,
    },
    protocol::{filterable::Filterable, items::Node, value::Value},
};

use super::super::tr_val::TraversalVal;

pub struct AddNIterator {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for AddNIterator {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait AddNAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn add_n(
        self,
        label: &'a str,
        properties: impl IntoIterator<Item = (String, Value)>,
        secondary_indices: Option<&'a [String]>,
        id: Option<String>,
    ) -> Self;
}

impl<'a> AddNAdapter<'a>
    for RwTraversalIterator<'a, std::iter::Once<Result<TraversalVal, GraphError>>>
{
    fn add_n(
        self,
        label: &'a str,
        properties: impl IntoIterator<Item = (String, Value)>,
        secondary_indices: Option<&'a [String]>,
        id: Option<String>,
    ) -> Self {
        let node = Node {
            id: id.unwrap_or(Uuid::new_v4().to_string()),
            label: label.to_string(),
            properties: properties.into_iter().collect(),
        };
        let secondary_indices = secondary_indices.unwrap_or(&[]).to_vec();
        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);
        match bincode::serialize(&node) {
            Ok(bytes) => {
                if let Err(e) = self.storage.nodes_db.put(
                    self.txn,
                    &HelixGraphStorage::node_key(&node.id),
                    &bytes.clone(),
                ) {
                    result = Err(GraphError::from(e));
                }
            }
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
                            if let Err(e) = db.put(self.txn, &serialized, node.id.as_bytes()) {
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

        if result.is_ok() {
            result = Ok(TraversalVal::Node(node.clone()));
        } else {
            result = Err(GraphError::New(format!(
                "Failed to add node to secondary indices"
            )));
        }

        RwTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
