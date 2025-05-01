use super::super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        storage_core::storage_core::HelixGraphStorage, types::GraphError, vector_core::hnsw::HNSW,
    },
    protocol::{
        items::{v6_uuid, Edge, SerializedEdge},
        label_hash::hash_label,
        value::Value,
    },
};
use heed3::PutFlags;
use uuid::Uuid;

pub enum EdgeType {
    Vec,
    Std,
}
pub struct AddE {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for AddE {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait AddEAdapter<'a, 'b>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn add_e(
        self,
        label: &'a str,
        properties: Vec<(String, Value)>,
        id: Option<u128>,
        from_node: u128,
        to_node: u128,
        should_check: bool,
        edge_type: EdgeType,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>;

    fn node_vec_exists(&self, node_vec_id: &u128, edge_type: EdgeType) -> bool;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>>> AddEAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn add_e(
        self,
        label: &'a str,
        properties: Vec<(String, Value)>,
        id: Option<u128>,
        from_node: u128,
        to_node: u128,
        should_check: bool,
        edge_type: EdgeType,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let edge = Edge {
            id: id.unwrap_or(v6_uuid()),
            label: label.to_string(),
            properties: properties.into_iter().collect(),
            from_node,
            to_node,
        };

        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);

        if let EdgeType::Std = edge_type {
            if should_check {
                if !(self.node_vec_exists(&from_node, EdgeType::Std)
                    && self.node_vec_exists(&to_node, EdgeType::Std))
                {
                    result = Err(GraphError::NodeNotFound);
                }
            }
        }
        
        match SerializedEdge::encode_edge(&edge) {
            Ok(bytes) => {
                if let Err(e) = self.storage.edges_db.put_with_flags(
                    self.txn,
                    PutFlags::APPEND,
                    &HelixGraphStorage::edge_key(&edge.id),
                    &bytes,
                ) {
                    result = Err(GraphError::from(e));
                }
            }
            Err(e) => result = Err(GraphError::from(e)),
        }

        let label_hash = hash_label(edge.label.as_str(), None);

        match self.storage.out_edges_db.put(
            self.txn,
            &HelixGraphStorage::out_edge_key(&from_node, &label_hash),
            &HelixGraphStorage::pack_edge_data(&to_node, &edge.id),
        ) {
            Ok(_) => {}
            Err(e) => {
                println!("add_e => error adding out edge: {:?}", e);
                result = Err(GraphError::from(e));
            }
        }

        match self.storage.in_edges_db.put(
            self.txn,
            &HelixGraphStorage::in_edge_key(&to_node, &label_hash),
            &HelixGraphStorage::pack_edge_data(&from_node, &edge.id),
        ) {
            Ok(_) => {}
            Err(e) => {
                println!("add_e => error adding in edge: {:?}", e);
                result = Err(GraphError::from(e));
            }
        }

        let result = match result {
            Ok(_) => Ok(TraversalVal::Edge(edge)),
            Err(_) => Err(GraphError::EdgeNotFound),
        };

        RwTraversalIterator {
            inner: std::iter::once(result), // TODO: change to support adding multiple edges
            storage: self.storage,
            txn: self.txn,
        }
    }

    fn node_vec_exists(&self, node_vec_id: &u128, edge_type: EdgeType) -> bool {
        let exists = match edge_type {
            EdgeType::Std => self
                .storage
                .nodes_db
                .get(self.txn, &HelixGraphStorage::node_key(&node_vec_id))
                .map_or(false, |node| node.is_none()),
            EdgeType::Vec => self
                .storage
                .vectors
                .get_vector(self.txn, *node_vec_id, 0, false)
                .is_ok(),
        };

        if !exists {
            return false;
        }

        true
    }
}
