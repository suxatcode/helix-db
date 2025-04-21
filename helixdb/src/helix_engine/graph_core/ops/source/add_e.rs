use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        storage_core::storage_core::HelixGraphStorage, types::GraphError,
    },
    protocol::{items::Edge, value::Value},
};

use super::super::tr_val::TraversalVal;

pub struct AddE {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for AddE {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait AddEAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn add_e(
        self,
        label: &'a str,
        properties: impl IntoIterator<Item = (String, Value)>,
        id: Option<u128>,
        from_node: u128,
        to_node: u128,
    ) -> impl Iterator<Item = Result<TraversalVal, GraphError>>;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>>> AddEAdapter<'a>
    for RwTraversalIterator<'a, 'b, I>
{
    fn add_e(
        self,
        label: &'a str,
        properties: impl IntoIterator<Item = (String, Value)>,
        id: Option<u128>,
        from_node: u128,
        to_node: u128,
    ) -> impl Iterator<Item = Result<TraversalVal, GraphError>> {
        let edge = Edge {
            id: id.unwrap_or(Uuid::new_v4().as_u128()),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
            from_node,
            to_node,
        };
        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);
        if self
            .storage
            .nodes_db
            .get(
                self.txn,
                &HelixGraphStorage::node_key(&edge.from_node),
            )
            .map_or(false, |node| node.is_none())
            || self
                .storage
                .nodes_db
                .get(
                    self.txn,
                    &HelixGraphStorage::node_key(&edge.to_node),
                )
                .map_or(false, |node| node.is_none())
        {
            result = Err(GraphError::NodeNotFound);
        }

        match bincode::serialize(&edge) {
            Ok(bytes) => {
                if let Err(e) = self.storage.edges_db.put(
                    self.txn,
                    &HelixGraphStorage::edge_key(&edge.id),
                    &bytes.clone(),
                ) {
                    result = Err(GraphError::from(e));
                }
            }
            Err(e) => result = Err(GraphError::from(e)),
        }

        match self.storage.edge_labels_db.put(
            self.txn,
            &HelixGraphStorage::edge_label_key(&edge.label, Some(&edge.id)),
            &(),
        ) {
            Ok(_) => {}
            Err(e) => result = Err(GraphError::from(e)),
        }

        match self.storage.out_edges_db.put(
            self.txn,
            &HelixGraphStorage::out_edge_key(&edge.from_node, &edge.label, Some(&edge.to_node)),
            &edge.id.to_le_bytes(),
        ) {
            Ok(_) => {}
            Err(e) => result = Err(GraphError::from(e)),
        }

        match self.storage.in_edges_db.put(
            self.txn,
            &HelixGraphStorage::in_edge_key(&edge.from_node, &edge.label, Some(&edge.to_node)),
            &edge.id.to_le_bytes(),
        ) {
            Ok(_) => {}
            Err(e) => result = Err(GraphError::from(e)),
        }
        if result.is_ok() {
            result = Ok(TraversalVal::Edge(edge));
        } else {
            result = Err(GraphError::EdgeNotFound)
        }
        RwTraversalIterator {
            inner: std::iter::once(result), // TODO: change to support adding multiple edges
            storage: self.storage,
            txn: self.txn,
        }
    }
}
