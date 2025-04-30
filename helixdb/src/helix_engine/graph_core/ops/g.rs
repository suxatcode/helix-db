use std::{collections::HashMap, sync::Arc, time::Instant};

use heed3::{types::Bytes, PutFlags, RoTxn, RwTxn};
use itertools::Itertools;

use crate::{
    decode_str,
    helix_engine::{
        graph_core::traversal_iter::{RoTraversalIterator, RwTraversalIterator},
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node}, label_hash::hash_label,
    },
};

use super::tr_val::{Traversable, TraversalVal};

pub struct G {
    iter: std::iter::Once<Result<TraversalVal, GraphError>>,
}

// implementing iterator for OutIterator
impl Iterator for G {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl G {
    pub fn new<'a>(
        storage: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        Self: Sized,
    {
        let iter = std::iter::once(Ok(TraversalVal::Empty));
        RoTraversalIterator {
            inner: iter,
            storage,
            txn,
        }
    }

    pub fn new_mut<'a, 'b>(
        storage: Arc<HelixGraphStorage>,
        txn: &'b mut RwTxn<'a>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        Self: Sized,
    {
        let iter = std::iter::once(Ok(TraversalVal::Empty));
        RwTraversalIterator {
            inner: iter,
            storage,
            txn,
        }
    }

    pub fn new_from<'a>(
        storage: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
        vals: Vec<TraversalVal>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        RoTraversalIterator {
            inner: vals.into_iter().map(|val| Ok(val)),
            storage,
            txn,
        }
    }

    pub fn bulk_add_e(
        storage: Arc<HelixGraphStorage>,
        edges: &mut [Edge],
        should_check_nodes: bool,
        chunk_size: usize,
    ) -> Result<(), GraphError> {
        // sort by id
        let mut txn = storage.graph_env.write_txn()?;

        let mut out_edges_buffer = Vec::with_capacity(edges.len());
        let mut in_edges_buffer = Vec::with_capacity(edges.len());
        let mut label_hashes= HashMap::new();
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
        in_edges_buffer.extend(edges.iter().map(|edge| {
            let label_hash = match label_hashes.get(&edge.label) {
                Some(hash) => *hash,
                None => {
                    let hash = hash_label(edge.label.as_str(), None);
                    label_hashes.insert(edge.label.clone(), hash);
                    hash
                }
            };
            (edge.to_node, label_hash, edge.from_node, edge.id)
        }));
        println!("separated");
        edges.sort_unstable();
        out_edges_buffer.sort_unstable();
        in_edges_buffer.sort_unstable();
        println!(
            "sorted edges: {:?}, out edges: {:?}, in edges: {:?}",
            edges.len(),
            out_edges_buffer.len(),
            in_edges_buffer.len()
        );
        let zipped = edges
            .iter()
            .zip(out_edges_buffer.iter().zip(in_edges_buffer.iter()))
            .chunks(chunk_size);
        let mut prev_out = None;
        let mut prev_in = None;
        println!("zipped and chunked");
        let mut start = Instant::now();
        for chunk in &zipped {
            for (edge, (out, in_)) in chunk {
                // EDGES
                if should_check_nodes
                    && (storage
                        .nodes_db
                        .get(&mut txn, &HelixGraphStorage::node_key(&edge.from_node))
                        .map_or(false, |node| node.is_none())
                        || storage
                            .nodes_db
                            .get(&mut txn, &HelixGraphStorage::node_key(&edge.to_node))
                            .map_or(false, |node| node.is_none()))
                {
                    return Err(GraphError::NodeNotFound);
                }
                match bincode::serialize(&edge) {
                    Ok(bytes) => {
                        if let Err(e) = storage.edges_db.put_with_flags(
                            &mut txn,
                            PutFlags::APPEND,
                            &HelixGraphStorage::edge_key(&edge.id),
                            &bytes,
                        ) {
                            println!("error adding edge: {:?}", e);
                            return Err(GraphError::from(format!("error adding edge: {:?}", e)));
                        }
                    }
                    Err(e) => {
                        println!("error serializing edge: {:?}", e);
                        return Err(GraphError::from(format!("error serializing edge: {:?}", e)));
                    }
                }

                match storage.edge_labels_db.put_with_flags(
                    &mut txn,
                    PutFlags::APPEND,
                    label_hashes.get(&edge.label).unwrap(),
                    &edge.id.to_be_bytes(),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error adding edge label: {:?}", e);
                        return Err(GraphError::from(format!(
                            "error adding edge label: {:?}",
                            e
                        )));
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

                match storage.out_edges_db.put_with_flags(
                    &mut txn,
                    out_flag,
                    &HelixGraphStorage::out_edge_key(&from_node, &label),
                    &HelixGraphStorage::pack_edge_data(&to_node, &id),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error adding out edge: {:?}", e);
                        return Err(GraphError::from(format!("error adding out edge: {:?}", e)));
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

                match storage.in_edges_db.put_with_flags(
                    &mut txn,
                    in_flag,
                    &HelixGraphStorage::in_edge_key(&to_node, &label),
                    &HelixGraphStorage::pack_edge_data(&from_node, &id),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error adding in edge: {:?}", e);
                        return Err(GraphError::from(format!("error adding in edge: {:?}", e)));
                    }
                }
            }
            txn.commit()?;
            println!("processed {} edges in {:?}", chunk_size, start.elapsed());
            start = Instant::now();
            txn = storage.graph_env.write_txn()?;
        }
        txn.commit()?;
        Ok(())
    }
}
