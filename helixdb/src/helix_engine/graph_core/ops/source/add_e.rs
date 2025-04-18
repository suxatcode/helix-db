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

pub struct AddE<'a> {
    storage: &'a Arc<HelixGraphStorage>,
    txn: &'a mut RwTxn<'a>,
    edge: Edge,
}

impl<'a> Iterator for AddE<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self
            .storage
            .nodes_db
            .get(
                self.txn,
                HelixGraphStorage::node_key(&self.edge.from_node).as_slice(),
            )
            .map_or(false, |node| node.is_none())
            || self
                .storage
                .nodes_db
                .get(
                    self.txn,
                    HelixGraphStorage::node_key(&self.edge.to_node).as_slice(),
                )
                .map_or(false, |node| node.is_none())
        {
            return Some(Err(GraphError::NodeNotFound));
        }

        match bincode::serialize(&self.edge) {
            Ok(bytes) => {
                if let Err(e) = self.storage.edges_db.put(
                    self.txn,
                    &HelixGraphStorage::edge_key(&self.edge.id),
                    &bytes.clone(),
                ) {
                    return Some(Err(GraphError::from(e)));
                }
            }
            Err(e) => return Some(Err(GraphError::from(e))),
        }

        match self.storage.edge_labels_db.put(
            self.txn,
            &HelixGraphStorage::edge_label_key(&self.edge.label, &self.edge.id),
            &(),
        ) {
            Ok(_) => {}
            Err(e) => return Some(Err(GraphError::from(e))),
        }

        match self.storage.out_edges_db.put(
            self.txn,
            &HelixGraphStorage::out_edge_key(&self.edge.from_node, &self.edge.to_node),
            &self.edge.id.as_bytes(),
        ) {
            Ok(_) => {}
            Err(e) => return Some(Err(GraphError::from(e))),
        }

        match self.storage.in_edges_db.put(
            self.txn,
            &HelixGraphStorage::in_edge_key(&self.edge.from_node, &self.edge.to_node),
            &self.edge.id.as_bytes(),
        ) {
            Ok(_) => {}
            Err(e) => return Some(Err(GraphError::from(e))),
        }

        Some(Ok(TraversalVal::Edge(self.edge.clone()))) // TODO: Look into way to remove clone
    }
}

impl<'a> AddE<'a> {
    pub fn new(
        storage: &'a Arc<HelixGraphStorage>,
        txn: &'a mut RwTxn<'a>,
        label: &'a str,
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

        AddE { storage, txn, edge }
    }
}
