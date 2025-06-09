use std::collections::HashMap;
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use heed3::byteorder::BE;
use heed3::{
    types::*, Database, DatabaseFlags, Env, EnvOpenOptions, RoTxn as HeedRoTxn,
    RwTxn as HeedRwTxn, WithTls,
};

use super::{DbRoTxn, DbRwTxn, Storage};
use crate::helix_engine::bm25::bm25::{BM25, BM25Flatten, HBM25Config};
use crate::helix_engine::graph_core::config::Config;
use crate::helix_engine::types::GraphError;
use crate::helix_engine::vector_core::hnsw::HNSW;
use crate::helix_engine::vector_core::vector::HVector;
use crate::helix_engine::vector_core::vector_core::{HNSWConfig, VectorCore};
use crate::helix_engine::types::VectorError;
use crate::protocol::items::{Edge, Node, v6_uuid};
use crate::protocol::label_hash::hash_label;
use crate::protocol::value::Value;
use serde::Serialize;
use std::borrow::Cow;
use heed3::{BytesDecode, BytesEncode};

// region: Codecs
struct NodeCodec;

impl<'a> BytesEncode<'a> for NodeCodec {
    type EItem = Node;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn std::error::Error + Send + Sync>> {
        item.encode_node().map(Cow::Owned).map_err(|e| e.into())
    }
}

impl<'a> BytesDecode<'a> for NodeCodec {
    type DItem = Node;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn std::error::Error + Send + Sync>> {
        // The ID is not stored in the value, it's the key. We pass a dummy 0.
        Node::decode_node(bytes, 0).map_err(|e| e.into())
    }
}

struct EdgeCodec;

impl<'a> BytesEncode<'a> for EdgeCodec {
    type EItem = Edge;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn std::error::Error + Send + Sync>> {
        item.encode_edge().map(Cow::Owned).map_err(|e| e.into())
    }
}

impl<'a> BytesDecode<'a> for EdgeCodec {
    type DItem = Edge;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn std::error::Error + Send + Sync>> {
        // The ID is not stored in the value, it's the key. We pass a dummy 0.
        Edge::decode_edge(bytes, 0).map_err(|e| e.into())
    }
}

// endregion

// region: Transaction Wrappers
pub struct LmdbRoTxn<'a>(pub HeedRoTxn<'a>);

impl<'a> DbRoTxn<'a> for LmdbRoTxn<'a> {}

impl<'a> Deref for LmdbRoTxn<'a> {
    type Target = HeedRoTxn<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct LmdbRwTxn<'a>(pub HeedRwTxn<'a>);

impl<'a> DbRoTxn<'a> for LmdbRwTxn<'a> {}

impl<'a> DbRwTxn<'a> for LmdbRwTxn<'a> {
    fn commit(self) -> Result<(), GraphError> {
        self.0.commit().map_err(GraphError::from)
    }
}

impl<'a> Deref for LmdbRwTxn<'a> {
    type Target = HeedRwTxn<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for LmdbRwTxn<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a, 'b> From<&'a mut LmdbRwTxn<'b>> for LmdbRoTxn<'a> {
    fn from(txn: &'a mut LmdbRwTxn<'b>) -> Self {
        LmdbRoTxn(&txn.0)
    }
}
// endregion

// region: LmdbStorage Definition
pub struct LmdbStorage {
    pub graph_env: Env<WithTls>,
    pub nodes_db: Database<U128<BE>, NodeCodec>,
    pub edges_db: Database<U128<BE>, EdgeCodec>,
    pub out_edges_db: Database<Bytes, Bytes>,
    pub in_edges_db: Database<Bytes, Bytes>,
    pub secondary_indices: HashMap<String, Database<Bytes, U128<BE>>>,
    pub vectors: VectorCore,
    pub bm25: HBM25Config,
}

impl LmdbStorage {
    pub fn new(path: &str, config: Config) -> Result<LmdbStorage, GraphError> {
        fs::create_dir_all(path)?;

        let db_size = if config.db_max_size_gb.unwrap_or(100) >= 9999 {
            9998
        } else {
            config.db_max_size_gb.unwrap_or(100)
        };

        let graph_env = unsafe {
            EnvOpenOptions::new()
                .map_size(db_size * 1024 * 1024 * 1024)
                .max_dbs(20)
                .max_readers(200)
                .open(Path::new(path))?
        };

        let mut wtxn = graph_env.write_txn()?;

        let nodes_db = graph_env
            .database_options()
            .types::<U128<BE>, NodeCodec>()
            .name("nodes")
            .create(&mut wtxn)?;
        let edges_db = graph_env
            .database_options()
            .types::<U128<BE>, EdgeCodec>()
            .name("edges")
            .create(&mut wtxn)?;
        let out_edges_db: Database<Bytes, Bytes> = graph_env
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
            .name("out_edges")
            .create(&mut wtxn)?;
        let in_edges_db: Database<Bytes, Bytes> = graph_env
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
            .name("in_edges")
            .create(&mut wtxn)?;

        let mut secondary_indices = HashMap::new();
        if let Some(indexes) = config.graph_config.secondary_indices {
            for index in indexes {
                secondary_indices.insert(
                    index.clone(),
                    graph_env
                        .database_options()
                        .types::<Bytes, U128<BE>>()
                        .flags(DatabaseFlags::DUP_SORT)
                        .name(&index)
                        .create(&mut wtxn)?,
                );
            }
        }

        let vectors = VectorCore::new(
            &graph_env,
            &mut wtxn,
            HNSWConfig::new(
                config.vector_config.m,
                config.vector_config.ef_construction,
                config.vector_config.ef_search,
            ),
        )?;
        let bm25 = HBM25Config::new(&graph_env, &mut wtxn)?;

        wtxn.commit()?;
        Ok(Self {
            graph_env,
            nodes_db,
            edges_db,
            out_edges_db,
            in_edges_db,
            secondary_indices,
            vectors,
            bm25,
        })
    }

    // Helper methods from HelixGraphStorage
    #[inline(always)]
    pub fn node_key(id: &u128) -> &u128 {
        id
    }

    #[inline(always)]
    pub fn edge_key(id: &u128) -> &u128 {
        id
    }

    #[inline(always)]
    pub fn out_edge_key(from_node_id: &u128, label: &[u8; 4]) -> [u8; 20] {
        let mut key = [0u8; 20];
        key[0..16].copy_from_slice(&from_node_id.to_be_bytes());
        key[16..20].copy_from_slice(label);
        key
    }

    #[inline(always)]
    pub fn in_edge_key(to_node_id: &u128, label: &[u8; 4]) -> [u8; 20] {
        let mut key = [0u8; 20];
        key[0..16].copy_from_slice(&to_node_id.to_be_bytes());
        key[16..20].copy_from_slice(label);
        key
    }
    
    #[inline(always)]
    pub fn pack_edge_data(node_id: &u128, edge_id: &u128) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0..16].copy_from_slice(&edge_id.to_be_bytes());
        key[16..32].copy_from_slice(&node_id.to_be_bytes());
        key
    }

    #[inline(always)]
    pub fn unpack_adj_edge_data(data: &[u8]) -> Result<(u128, u128), GraphError> {
        let edge_id = u128::from_be_bytes(
            data[0..16]
                .try_into()
                .map_err(|_| GraphError::SliceLengthError)?,
        );
        let node_id = u128::from_be_bytes(
            data[16..32]
                .try_into()
                .map_err(|_| GraphError::SliceLengthError)?,
        );
        Ok((node_id, edge_id))
    }

    /// Get a reference to the secondary index database by name
    #[inline(always)]
    pub fn get_node_index_db(&self, index: &str) -> Result<&Database<Bytes, U128<BE>>, GraphError> {
        self.secondary_indices
            .get(index)
            .ok_or_else(|| GraphError::New(format!("Secondary Index {} not found", index)))
    }
}
// endregion

// region: Storage Trait Implementation
impl Storage for LmdbStorage {
    type RoTxn<'a> = LmdbRoTxn<'a>;
    type RwTxn<'a> = LmdbRwTxn<'a>;

    fn ro_txn(&self) -> Result<Self::RoTxn<'_>, GraphError> {
        self.graph_env.read_txn().map(|txn| LmdbRoTxn(txn)).map_err(Into::into)
    }

    fn rw_txn(&self) -> Result<Self::RwTxn<'_>, GraphError> {
        self.graph_env.write_txn().map(|txn| LmdbRwTxn(txn)).map_err(Into::into)
    }

    fn create_secondary_index(&mut self, name: &str) -> Result<(), GraphError> {
        let mut wtxn = self.graph_env.write_txn()?;
        let db = self.graph_env.create_database(&mut wtxn, Some(name))?;
        wtxn.commit()?;
        self.secondary_indices.insert(name.to_string(), db);
        Ok(())
    }

    fn drop_secondary_index(&mut self, name: &str) -> Result<(), GraphError> {
        let mut wtxn = self.graph_env.write_txn()?;
        let db = self
            .secondary_indices
            .get(name)
            .ok_or_else(|| GraphError::New(format!("Secondary Index {} not found", name)))?;
        db.clear(&mut wtxn)?;
        wtxn.commit()?;
        self.secondary_indices.remove(name);
        Ok(())
    }

    fn check_exists<'a>(&self, txn: &Self::RoTxn<'a>, id: &u128) -> Result<bool, GraphError> {
        let exists = self.nodes_db.get(txn, Self::node_key(id))?.is_some();
        Ok(exists)
    }

    fn get_node<'a>(&self, txn: &Self::RoTxn<'a>, id: &u128) -> Result<Node, GraphError> {
        self.nodes_db
            .get(txn, id)?
            .map(|mut node| {
                node.id = *id;
                node
            })
            .ok_or_else(|| GraphError::NodeNotFound(*id))
    }

    fn get_edge<'a>(&self, txn: &Self::RoTxn<'a>, id: &u128) -> Result<Edge, GraphError> {
        self.edges_db
            .get(txn, id)?
            .map(|mut edge| {
                edge.id = *id;
                edge
            })
            .ok_or_else(|| GraphError::EdgeNotFound(*id))
    }

    fn drop_node<'a>(&self, txn: &mut Self::RwTxn<'a>, id: &u128) -> Result<(), GraphError> {
        let out_edges = {
            let iter = self.out_edges_db.prefix_iter(txn, &id.to_be_bytes())?;
            let mut out_edges = Vec::with_capacity(iter.size_hint().0);
            for result in iter {
                let (key, value) = result?;
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (_, edge_id) = Self::unpack_adj_edge_data(value)?;
                out_edges.push((edge_id, label));
            }
            out_edges
        };

        let in_edges = {
            let iter = self.in_edges_db.prefix_iter(txn, &id.to_be_bytes())?;
            let mut in_edges = Vec::with_capacity(iter.size_hint().0);
            for result in iter {
                let (key, value) = result?;
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (node_id, edge_id) = Self::unpack_adj_edge_data(value)?;
                in_edges.push((edge_id, label, node_id));
            }
            in_edges
        };

        for (out_edge_id, label_bytes) in out_edges.iter() {
            self.edges_db.delete(txn, Self::edge_key(out_edge_id))?;
            self.out_edges_db
                .delete(txn, &Self::out_edge_key(id, label_bytes))?;
        }
        for (in_edge_id, label_bytes, other_id) in in_edges.iter() {
            self.edges_db.delete(txn, Self::edge_key(in_edge_id))?;
            self.in_edges_db
                .delete(txn, &Self::in_edge_key(other_id, label_bytes))?;
        }

        self.nodes_db.delete(txn, Self::node_key(id))?;
        Ok(())
    }

    fn drop_edge<'a>(&self, txn: &mut Self::RwTxn<'a>, edge_id: &u128) -> Result<(), GraphError> {
        let mut edge = self
            .edges_db
            .get(txn, Self::edge_key(edge_id))?
            .ok_or_else(|| GraphError::EdgeNotFound(*edge_id))?;
        edge.id = *edge_id;
        let label_hash = hash_label(&edge.label, None);
        
        self.edges_db.delete(txn, Self::edge_key(edge_id))?;
        self.out_edges_db
            .delete(txn, &Self::out_edge_key(&edge.from_node, &label_hash))?;
        self.in_edges_db
            .delete(txn, &Self::in_edge_key(&edge.to_node, &label_hash))?;

        Ok(())
    }

    fn update_node<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        id: &u128,
        properties: &Value,
    ) -> Result<Node, GraphError> {
        let mut old_node = self.get_node(&LmdbRoTxn::from(txn), id)?;
        if let Value::Object(props) = properties {
            old_node.properties = Some(props.clone());
        }
        self.nodes_db.put(txn, id, &old_node)?;
        Ok(old_node)
    }

    fn update_edge<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        id: &u128,
        properties: &Value,
    ) -> Result<Edge, GraphError> {
        let mut old_edge = self.get_edge(&LmdbRoTxn::from(txn), id)?;
        if let Value::Object(props) = properties {
            old_edge.properties = Some(props.clone());
        }
        self.edges_db.put(txn, id, &old_edge)?;
        Ok(old_edge)
    }

    fn add_edge<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        label: &str,
        properties: Option<Vec<(String, Value)>>,
        from_node: u128,
        to_node: u128,
    ) -> Result<Edge, GraphError> {
        // TODO: check if nodes exist first

        let edge = Edge {
            id: v6_uuid(),
            label: label.to_string(),
            properties: properties.map(|props| props.into_iter().collect()),
            from_node,
            to_node,
        };

        self.edges_db
            .put(txn, &Self::edge_key(&edge.id), &edge)?;

        let label_hash = hash_label(edge.label.as_str(), None);

        self.out_edges_db.put(
            txn,
            &Self::out_edge_key(&from_node, &label_hash),
            &Self::pack_edge_data(&to_node, &edge.id),
        )?;

        self.in_edges_db.put(
            txn,
            &Self::in_edge_key(&to_node, &label_hash),
            &Self::pack_edge_data(&from_node, &edge.id),
        )?;

        Ok(edge)
    }

    fn get_out_nodes<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        from_id: &u128,
    ) -> Result<Vec<Node>, GraphError> {
        let out_prefix = LmdbStorage::out_edge_key(from_id, &hash_label(edge_label, None));
        let mut nodes = Vec::new();

        for result in self.out_edges_db.prefix_iter(txn, &out_prefix)? {
            let (_, value) = result?;
            let (to_node_id, _) = LmdbStorage::unpack_adj_edge_data(value)?;
            let node = self.get_node(txn, &to_node_id)?;
            nodes.push(node);
        }

        Ok(nodes)
    }

    fn get_in_nodes<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        to_id: &u128,
    ) -> Result<Vec<Node>, GraphError> {
        let in_prefix = LmdbStorage::in_edge_key(to_id, &hash_label(edge_label, None));
        let mut nodes = Vec::new();

        for result in self.in_edges_db.prefix_iter(txn, &in_prefix)? {
            let (_, value) = result?;
            let (from_node_id, _) = LmdbStorage::unpack_adj_edge_data(value)?;
            let node = self.get_node(txn, &from_node_id)?;
            nodes.push(node);
        }

        Ok(nodes)
    }

    fn get_out_edges<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        from_id: &u128,
    ) -> Result<Vec<Edge>, GraphError> {
        let out_prefix = LmdbStorage::out_edge_key(from_id, &hash_label(edge_label, None));
        let mut edges = Vec::new();

        for result in self.out_edges_db.prefix_iter(txn, &out_prefix)? {
            let (_, value) = result?;
            let (_, edge_id) = LmdbStorage::unpack_adj_edge_data(value)?;
            let edge = self.get_edge(txn, &edge_id)?;
            edges.push(edge);
        }

        Ok(edges)
    }

    fn get_in_edges<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        to_id: &u128,
    ) -> Result<Vec<Edge>, GraphError> {
        let in_prefix = LmdbStorage::in_edge_key(to_id, &hash_label(edge_label, None));
        let mut edges = Vec::new();

        for result in self.in_edges_db.prefix_iter(txn, &in_prefix)? {
            let (_, value) = result?;
            let (_, edge_id) = LmdbStorage::unpack_adj_edge_data(value)?;
            let edge = self.get_edge(txn, &edge_id)?;
            edges.push(edge);
        }

        Ok(edges)
    }

    fn shortest_path<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        from_id: &u128,
        to_id: &u128,
    ) -> Result<(Vec<Node>, Vec<Edge>), GraphError> {
        let mut queue = std::collections::VecDeque::with_capacity(32);
        let mut visited = std::collections::HashSet::with_capacity(64);
        let mut parent: std::collections::HashMap<u128, (u128, Edge)> =
            std::collections::HashMap::with_capacity(32);
        queue.push_back(*from_id);
        visited.insert(*from_id);

        while let Some(current_id) = queue.pop_front() {
            let out_prefix =
                LmdbStorage::out_edge_key(&current_id, &hash_label(edge_label, None));

            for result in self.out_edges_db.prefix_iter(txn, &out_prefix)? {
                let (_, value) = result?;
                let (to_node, edge_id) = LmdbStorage::unpack_adj_edge_data(value)?;

                if !visited.contains(&to_node) {
                    visited.insert(to_node);
                    let edge = self.get_edge(txn, &edge_id)?;
                    parent.insert(to_node, (current_id, edge));

                    if to_node == *to_id {
                        let mut nodes = Vec::with_capacity(parent.len());
                        let mut edges = Vec::with_capacity(parent.len() - 1);
                        let mut current = to_id;

                        while current != from_id {
                            nodes.push(self.get_node(txn, current)?);
                            let (prev_node, edge) = &parent[current];
                            edges.push(edge.clone());
                            current = prev_node;
                        }
                        nodes.push(self.get_node(txn, from_id)?);
                        nodes.reverse();
                        edges.reverse();
                        return Ok((nodes, edges));
                    }
                    queue.push_back(to_node);
                }
            }
        }
        Err(GraphError::ShortestPathNotFound)
    }

    fn shortest_mutual_path<'a>(
        &self,
        _txn: &Self::RoTxn<'a>,
        _edge_label: &str,
        _from_id: &u128,
        _to_id: &u128,
    ) -> Result<(Vec<Node>, Vec<Edge>), GraphError> {
        unimplemented!("shortest_mutual_path is not part of the storage trait directly yet")
    }

    fn node_from_index<'a, K>(
        &self,
        txn: &Self::RoTxn<'a>,
        index: &str,
        key: K,
    ) -> Result<Option<Node>, GraphError>
    where
        K: Into<Value> + Serialize,
    {
        let db = self.get_node_index_db(index)?;
        let key_bytes = bincode::serialize(&key.into())?;

        let result = db.get(txn, &key_bytes)?;

        if let Some(node_id) = result {
            return self.get_node(txn, &node_id).map(Some);
        }

        Ok(None)
    }

    fn index_node<'a, K>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        index: &str,
        key: K,
        node: &Node,
    ) -> Result<(), GraphError>
    where
        K: Into<Value> + Serialize,
    {
        let db = self.get_node_index_db(index)?;
        let key_bytes = bincode::serialize(&key.into())?;
        db.put(txn, &key_bytes, &node.id)?;
        Ok(())
    }

    fn add_node<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        label: &str,
        properties: Option<Vec<(String, Value)>>,
        secondary_indices: Option<&[&str]>,
    ) -> Result<Node, GraphError> {
        let node = Node {
            id: v6_uuid(),
            label: label.to_string(),
            properties: properties.map(|props| props.into_iter().collect()),
        };

        self.nodes_db.put(txn, &node.id, &node)?;

        if let Some(indices) = secondary_indices {
            for index_name in indices {
                let db = self.secondary_indices.get(*index_name).ok_or_else(|| {
                    GraphError::New(format!("Secondary Index {} not found", index_name))
                })?;

                if let Some(value) = node.properties.as_ref().and_then(|p| p.get(*index_name)) {
                    let key_bytes = bincode::serialize(value)?;
                    db.put(txn, &key_bytes, &node.id)?;
                }
            }
        }

        if let Some(props) = &node.properties {
            let data = props.flatten_bm25();
            if !data.is_empty() {
                self.bm25.insert_doc(txn, node.id, &data)?;
            }
        }

        Ok(node)
    }

    fn get_all_nodes<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<Node, GraphError>> + 'a>, GraphError> {
        Ok(Box::new(self.nodes_db.iter(txn)?.map(|item| {
            item.map(|(id, mut node)| {
                node.id = id;
                node
            })
            .map_err(GraphError::from)
        })))
    }

    fn get_all_edges<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<Edge, GraphError>> + 'a>, GraphError> {
        Ok(Box::new(self.edges_db.iter(txn)?.map(|item| {
            item.map(|(id, mut edge)| {
                edge.id = id;
                edge
            })
            .map_err(GraphError::from)
        })))
    }

    // Vector operations implementation
    fn search_vectors<'a, F>(
        &self,
        txn: &Self::RoTxn<'a>,
        query: &[f64],
        k: usize,
        filter: Option<&[F]>,
    ) -> Result<Vec<HVector>, VectorError>
    where
        F: Fn(&HVector, &Self::RoTxn<'a>) -> bool,
    {
        // Convert our transaction to the HEED transaction that HNSW expects
        self.vectors.search(&txn.0, query, k, filter, false)
    }

    fn insert_vector<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        data: &[f64],
        fields: Option<Vec<(String, Value)>>,
    ) -> Result<HVector, VectorError> {
        // Convert our transaction to the HEED transaction that HNSW expects
        self.vectors.insert(&mut txn.0, data, fields)
    }

    fn get_vector<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        id: u128,
        level: usize,
        with_data: bool,
    ) -> Result<HVector, VectorError> {
        // Convert our transaction to the HEED transaction that HNSW expects
        self.vectors.get_vector(&txn.0, id, level, with_data)
    }

    fn get_all_vectors<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        level: Option<usize>,
    ) -> Result<Vec<HVector>, VectorError> {
        // Convert our transaction to the HEED transaction that HNSW expects
        self.vectors.get_all_vectors(&txn.0, level)
    }

    // BM25 operations implementation
    fn insert_bm25_doc<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        doc_id: u128,
        doc: &str,
    ) -> Result<(), GraphError> {
        // Convert our transaction to the HEED transaction that BM25 expects
        self.bm25.insert_doc(&mut txn.0, doc_id, doc)
    }

    fn update_bm25_doc<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        doc_id: u128,
        doc: &str,
    ) -> Result<(), GraphError> {
        // Convert our transaction to the HEED transaction that BM25 expects
        self.bm25.update_doc(&mut txn.0, doc_id, doc)
    }

    fn delete_bm25_doc<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        doc_id: u128,
    ) -> Result<(), GraphError> {
        // Convert our transaction to the HEED transaction that BM25 expects
        self.bm25.delete_doc(&mut txn.0, doc_id)
    }

    fn search_bm25<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError> {
        // Convert our transaction to the HEED transaction that BM25 expects
        self.bm25.search(&txn.0, query, limit)
    }
}
// endregion 