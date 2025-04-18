use std::{collections::HashMap, iter::Once, sync::Arc};

use heed3::{RoTxn, RwTxn};
use uuid::Uuid;

use crate::{
    helix_engine::{storage_core::storage_core::HelixGraphStorage, types::GraphError},
    protocol::{filterable::Filterable, items::Node, value::Value},
};

use super::super::tr_val::TraversalVal;

pub struct AddN {
    result: Option<Result<TraversalVal, GraphError>>,
}

impl Iterator for AddN {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.result.take()
    }
}

impl AddN {
    pub fn new(
        storage: &Arc<HelixGraphStorage>,
        txn: &mut RwTxn,
        label: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
        secondary_indices: Option<&[String]>,
        id: Option<String>,
    ) -> Self {
        let node = Node {
            id: id.unwrap_or(Uuid::new_v4().to_string()),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
        };
        let mut result: Option<Result<TraversalVal, GraphError>> = None;
        match bincode::serialize(&node) {
            Ok(bytes) => {
                if let Err(e) = storage.nodes_db.put(
                    txn,
                    &HelixGraphStorage::node_key(&node.id),
                    &bytes.clone(),
                ) {
                    result = Some(Err(GraphError::from(e)));
                }
            }
            Err(e) => result = Some(Err(GraphError::from(e))),
        }

        for index in secondary_indices.unwrap_or(&[]) {
            match storage.secondary_indices.get(index) {
                Some(db) => {
                    let key = match node.check_property(index) {
                        Some(value) => value,
                        None => {
                            result = Some(Err(GraphError::New(format!(
                                "Secondary Index {} not found",
                                index
                            ))));
                            continue;
                        }
                    };
                    match bincode::serialize(&key) {
                        Ok(serialized) => {
                            if let Err(e) = db.put(txn, &serialized, node.id.as_bytes()) {
                                result = Some(Err(GraphError::from(e)));
                            }
                        }
                        Err(e) => result = Some(Err(GraphError::from(e))),
                    }
                }
                None => {
                    result = Some(Err(GraphError::New(format!(
                        "Secondary Index {} not found",
                        index
                    ))))
                }
            }
        }

        if result.is_none() {
            result = Some(Ok(TraversalVal::Node(node)));
        }

        AddN { result }
    }
}
