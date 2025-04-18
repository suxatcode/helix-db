use std::{collections::HashMap, iter::Once, sync::Arc};

use heed3::{RoTxn, RwTxn};
use uuid::Uuid;

use crate::{
    helix_engine::{storage_core::storage_core::HelixGraphStorage, types::GraphError},
    protocol::{
        filterable::Filterable,
        items::{Edge, Node},
        value::Value,
    },
};

use super::super::tr_val::TraversalVal;

pub struct AddE {
    result: Option<Result<TraversalVal, GraphError>>,
}

impl Iterator for AddE {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.result.take()
    }
}

impl AddE {
    pub fn new(
        storage: &Arc<HelixGraphStorage>,
        txn: &mut RwTxn,
        label: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
        id: Option<String>,
        from_node: String,
        to_node: String,
    ) -> Self {
        let edge = Edge {
            id: id.unwrap_or(Uuid::new_v4().to_string()),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
            from_node,
            to_node,
        };
        let mut result: Option<Result<TraversalVal, GraphError>> = None;
        if storage
            .nodes_db
            .get(txn, HelixGraphStorage::node_key(&edge.from_node).as_slice())
            .map_or(false, |node| node.is_none())
            || storage
                .nodes_db
                .get(txn, HelixGraphStorage::node_key(&edge.to_node).as_slice())
                .map_or(false, |node| node.is_none())
        {
            result = Some(Err(GraphError::NodeNotFound));
        }

        match bincode::serialize(&edge) {
            Ok(bytes) => {
                if let Err(e) = storage.edges_db.put(
                    txn,
                    &HelixGraphStorage::edge_key(&edge.id),
                    &bytes.clone(),
                ) {
                    result = Some(Err(GraphError::from(e)));
                }
            }
            Err(e) => result = Some(Err(GraphError::from(e))),
        }

        match storage.edge_labels_db.put(
            txn,
            &HelixGraphStorage::edge_label_key(&edge.label, &edge.id),
            &(),
        ) {
            Ok(_) => {}
            Err(e) => result = Some(Err(GraphError::from(e))),
        }

        match storage.out_edges_db.put(
            txn,
            &HelixGraphStorage::out_edge_key(&edge.from_node, &edge.to_node),
            &edge.id.as_bytes(),
        ) {
            Ok(_) => {}
            Err(e) => result = Some(Err(GraphError::from(e))),
        }

        match storage.in_edges_db.put(
            txn,
            &HelixGraphStorage::in_edge_key(&edge.from_node, &edge.to_node),
            &edge.id.as_bytes(),
        ) {
            Ok(_) => {}
            Err(e) => result = Some(Err(GraphError::from(e))),
        }
        if result.is_none() {
            result = Some(Ok(TraversalVal::Edge(edge)));
        }
        AddE { result }
    }
}
