use std::sync::Arc;

use crate::{
    helix_engine::{storage_core::{
        storage_core::HelixGraphStorage, storage_methods::StorageMethods,
    }, types::GraphError},
    protocol::value::Value,
};

use super::tr_val::TraversalVal;
use heed3::RwTxn;

pub struct Update<'a, I> {
    iter: I,
    txn: &'a mut RwTxn<'a>,
    storage: &'a Arc<HelixGraphStorage>,
    props: Vec<(String, Value)>,
}

impl<'a, I> Iterator for Update<'a, I>
where
    I: Iterator<Item = TraversalVal>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => Some(match item {
                TraversalVal::Node(node) => {
                    match self.storage.get_node(self.txn, &node.id) {
                        Ok(mut old_node) => {
                            for (k, v) in self.props.iter() {
                                old_node.properties.insert(k.clone(), v.clone());
                            }
                            for (key, v) in old_node.properties.iter() {
                                if let Some(db) = self.storage.secondary_indices.get(key) {
                                    match bincode::serialize(v) {
                                        Ok(serialized) => {
                                            if let Err(e) = db.put(self.txn, &serialized, node.id.as_bytes()) {
                                                return Some(Err(GraphError::from(e)));
                                            }
                                        }
                                        Err(e) => return Some(Err(GraphError::from(e))),
                                    }
                                }
                            }
                            match bincode::serialize(&node) {
                                Ok(serialized) => {
                                    match self.storage.nodes_db.put(
                                        self.txn,
                                        &HelixGraphStorage::node_key(&node.id),
                                        &serialized,
                                    ) {
                                        Ok(_) => Ok(TraversalVal::Node(old_node)),
                                        Err(e) => Err(GraphError::from(e)),
                                    }
                                }
                                Err(e) => Err(GraphError::from(e)),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                TraversalVal::Edge(edge) => match self.storage.get_edge(self.txn, &edge.id) {
                    Ok(mut old_edge) => {
                        for (k, v) in self.props.iter() {
                            old_edge.properties.insert(k.clone(), v.clone());
                        }
                        match bincode::serialize(&edge) {
                            Ok(serialized) => {
                                match self.storage.nodes_db.put(
                                    self.txn,
                                    &HelixGraphStorage::edge_key(&edge.id),
                                    &serialized,
                                ) {
                                    Ok(_) => Ok(TraversalVal::Edge(old_edge)),
                                    Err(e) => Err(GraphError::from(e)),
                                }
                            }
                            Err(e) => Err(GraphError::from(e)),
                        }
                    }
                    Err(e) => Err(e),
                },
                _ => return None,
            }),
            None => None,
        }
    }
}
