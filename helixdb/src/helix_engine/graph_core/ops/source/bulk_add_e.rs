use heed3::PutFlags;
use itertools::Itertools;
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        storage_core::storage_core::HelixGraphStorage, types::GraphError,
    },
    protocol::{items::Edge, label_hash::hash_label, value::Value},
};

use super::super::tr_val::TraversalVal;

pub struct BulkAddE {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for BulkAddE {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait BulkAddEAdapter<'a, 'b>:
    Iterator<Item = Result<TraversalVal, GraphError>> + Sized
{
    fn bulk_add_e(
        self,
        edges: &mut [Edge],
        should_check_nodes: bool,
        chunk_size: usize,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>>> BulkAddEAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn bulk_add_e(
        self,
        edges: &mut [Edge],
        should_check_nodes: bool,
        chunk_size: usize,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);
        // sort by id
        edges.sort_unstable_by_key(|edge| edge.id);

        let mut out_edges_buffer = Vec::with_capacity(edges.len());
        let mut in_edges_buffer = Vec::with_capacity(edges.len());
        let mut label_hashes = HashMap::new();
        out_edges_buffer.extend(edges.iter().map(|edge| {
            let label_hash = match label_hashes.get(&edge.label) {
                Some(hash) => *hash,
                None => {
                    let hash = hash_label(edge.label.as_str(), None);
                    label_hashes.insert(edge.label.clone(), hash);
                    hash
                }
            };
            (edge.from_node, label_hash, edge.to_node, edge.id)
        }));
        in_edges_buffer.extend(
            out_edges_buffer
                .iter()
                .map(|(from, label, to, id)| (*to, *label, *from, *id)),
        );

        out_edges_buffer.sort_unstable_by_key(|(from, _, _, _)| *from);
        in_edges_buffer.sort_unstable_by_key(|(to, _, _, _)| *to);
        let zipped = edges
            .iter()
            .zip(out_edges_buffer.iter().zip(in_edges_buffer.iter()))
            .chunks(chunk_size);
        let mut prev_out = None;
        let mut prev_in = None;
        for chunk in &zipped {
            for (edge, (out, in_)) in chunk {
                // EDGES
                if should_check_nodes
                    && (self
                        .storage
                        .nodes_db
                        .get(self.txn, &HelixGraphStorage::node_key(&edge.from_node))
                        .map_or(false, |node| node.is_none())
                        || self
                            .storage
                            .nodes_db
                            .get(self.txn, &HelixGraphStorage::node_key(&edge.to_node))
                            .map_or(false, |node| node.is_none()))
                {
                    result = Err(GraphError::NodeNotFound);
                }
                match bincode::serialize(&edge) {
                    Ok(bytes) => {
                        if let Err(e) = self.storage.edges_db.put_with_flags(
                            self.txn,
                            PutFlags::APPEND,
                            &HelixGraphStorage::edge_key(&edge.id),
                            &bytes,
                        ) {
                            println!("error adding edge: {:?}", e);
                            result = Err(GraphError::from(e));
                        }
                    }
                    Err(e) => {
                        println!("error serializing edge: {:?}", e);
                        result = Err(GraphError::from(e));
                    }
                }

                match self.storage.edge_labels_db.put_with_flags(
                    self.txn,
                    PutFlags::APPEND,
                    &HelixGraphStorage::edge_label_key(
                        &label_hashes.get(&edge.label).unwrap(),
                        Some(&edge.id),
                    ),
                    &(),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error adding edge label: {:?}", e);
                        result = Err(GraphError::from(e));
                    }
                }

                // OUT EDGES
                let (from_node, label, to_node, id) = out;
                let out_flag = if Some((from_node, label)) == prev_out {
                    PutFlags::APPEND_DUP
                } else {
                    prev_out = Some((from_node, label));
                    PutFlags::APPEND
                };

                match self.storage.out_edges_db.put_with_flags(
                    self.txn,
                    out_flag,
                    &HelixGraphStorage::out_edge_key(&from_node, &label),
                    &HelixGraphStorage::pack_edge_data(&to_node, &id),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error adding out edge: {:?}", e);
                        result = Err(GraphError::from(e));
                    }
                }

                // IN EDGES
                let (to_node, label, from_node, id) = in_;
                let in_flag = if Some((to_node, label)) == prev_in {
                    PutFlags::APPEND_DUP
                } else {
                    prev_in = Some((to_node, label));
                    PutFlags::APPEND
                };

                match self.storage.in_edges_db.put_with_flags(
                    self.txn,
                    in_flag,
                    &HelixGraphStorage::in_edge_key(&to_node, &label),
                    &HelixGraphStorage::pack_edge_data(&from_node, &id),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error adding in edge: {:?}", e);
                        result = Err(GraphError::from(e));
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
