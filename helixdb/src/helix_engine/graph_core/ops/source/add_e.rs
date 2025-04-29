use super::super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        storage_core::storage_core::HelixGraphStorage,
        vector_core::hnsw::HNSW,
        types::GraphError,
    },
    protocol::{
        items::Edge,
        value::Value
    },
};
use heed3::PutFlags;
use uuid::Uuid;

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
        from_is_vec: bool,
        to_is_vec: bool,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>;

    fn node_vec_exists(&self, node_vec_id: &u128, is_vec: bool) -> Result<(), GraphError>;
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
        from_is_vec: bool,
        to_is_vec: bool,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let edge = Edge {
            id: id.unwrap_or(Uuid::new_v4().as_u128()),
            label: label.to_string(),
            properties: properties.into_iter().collect(),
            from_node,
            to_node,
        };

        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);

        if let Err(err) = self.node_vec_exists(&edge.from_node, from_is_vec) {
            result = Err(err);
            println!(
                "could not find from-{}: {:?}",
                if from_is_vec { "vector" } else { "node" },
                &edge.from_node,
            );
        }

        if let Err(err) = self.node_vec_exists(&edge.to_node, to_is_vec) {
            result = Err(err);
            println!(
                "could not find to-{}: {:?}",
                if to_is_vec { "vector" } else { "node" },
                &edge.to_node,
            );
        }

        match bincode::serialize(&edge) {
            Ok(bytes) => {
                if let Err(e) = self.storage.edges_db.put(
                    self.txn,
                    &HelixGraphStorage::edge_key(&edge.id),
                    &bytes,
                ) {
                    result = Err(GraphError::from(e));
                }
            }
            Err(e) => result = Err(GraphError::from(e)),
        }

        let label_hash = HelixGraphStorage::hash_label(edge.label.as_str());
        match self.storage.edge_labels_db.put(
            self.txn,
            &HelixGraphStorage::edge_label_key(&label_hash, Some(&edge.id)),
            &(),
        ) {
            Ok(_) => {}
            Err(e) => result = Err(GraphError::from(e)),
        }

        match self.storage.out_edges_db.put_with_flags(
            self.txn,
            PutFlags::APPEND,
            &HelixGraphStorage::out_edge_key(&from_node, &label_hash),
            &HelixGraphStorage::pack_edge_data(&to_node, &edge.id),
        ) {
            Ok(_) => {}
            Err(e) => {
                println!("error adding out edge: {:?}", e);
                result = Err(GraphError::from(e));
            }
        }

        match self.storage.in_edges_db.put_with_flags(
            self.txn,
            PutFlags::APPEND,
            &HelixGraphStorage::in_edge_key(&to_node, &label_hash),
            &HelixGraphStorage::pack_edge_data(&from_node, &edge.id),
        ) {
            Ok(_) => {}
            Err(e) => {
                println!("error adding in edge: {:?}", e);
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

    fn node_vec_exists(&self, node_vec_id: &u128, is_vec: bool) -> Result<(), GraphError> {
        if !is_vec {
            if self
                .storage
                .nodes_db
                .get(self.txn, &HelixGraphStorage::node_key(&node_vec_id))
                .map_or(false, |node| node.is_none())
            {
                return Err(GraphError::NodeNotFound);
            }
        } else {
            if !self.storage.vectors.get_vector(self.txn, *node_vec_id, 0, false).is_ok() {
                return Err(GraphError::VectorError(node_vec_id.to_string()));
            }
        }

        Ok(())
    }
}