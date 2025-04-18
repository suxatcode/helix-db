use std::{collections::HashMap, iter::Once, sync::Arc};

use heed3::{RoTxn, RwTxn};
use uuid::Uuid;

use crate::{
    helix_engine::{storage_core::storage_core::HelixGraphStorage, types::GraphError},
    protocol::{filterable::Filterable, items::Node, value::Value},
};

use super::super::tr_val::TraversalVal;

pub struct AddN<'a> {
    storage: &'a Arc<HelixGraphStorage>,
    txn: &'a mut RwTxn<'a>,
    node: Node,
    secondary_indices: Option<&'a [String]>,
}

impl<'a> Iterator for AddN<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        match bincode::serialize(&self.node) {
            Ok(bytes) => {
                if let Err(e) = self.storage.nodes_db.put(
                    self.txn,
                    &HelixGraphStorage::node_key(&self.node.id),
                    &bytes.clone(),
                ) {
                    return Some(Err(GraphError::from(e)));
                }
            }
            Err(e) => return Some(Err(GraphError::from(e))),
        }

        for index in self.secondary_indices.unwrap_or(&[]) {
            match self.storage.secondary_indices.get(index) {
                Some(db) => {
                    let key = match self.node.check_property(index) {
                        Some(value) => value,
                        None => {
                            return Some(Err(GraphError::New(format!(
                                "Secondary Index {} not found",
                                index
                            ))))
                        }
                    };
                    match bincode::serialize(&key) {
                        Ok(serialized) => {
                            if let Err(e) = db.put(self.txn, &serialized, self.node.id.as_bytes()) {
                                return Some(Err(GraphError::from(e)));
                            }
                        }
                        Err(e) => return Some(Err(GraphError::from(e))),
                    }
                }
                None => {
                    return Some(Err(GraphError::New(format!(
                        "Secondary Index {} not found",
                        index
                    ))))
                }
            }
        }

        Some(Ok(TraversalVal::Node(self.node.clone()))) // TODO: Look into way to remove clone
    }
}

impl<'a> AddN<'a> {
    pub fn new(
        storage: &'a Arc<HelixGraphStorage>,
        txn: &'a mut RwTxn<'a>,
        label: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
        secondary_indices: Option<&'a [String]>,
        id: Option<String>,
    ) -> Self {
        let node = Node {
            id: id.unwrap_or(Uuid::new_v4().to_string()),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
        };

        AddN {
            storage,
            txn,
            node,
            secondary_indices,
        }
    }
}
