use crate::helix_engine::graph_core::config::Config;
use crate::helix_engine::vector_core::vector_core::{HNSWConfig, VectorCore};
use crate::protocol::filterable::Filterable;
use crate::protocol::items::{SerializedEdge, SerializedNode};

use heed3::{types::*, Database, Env, EnvOpenOptions, RoTxn, RwTxn, WithTls};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;
use uuid::Uuid;

use crate::helix_engine::storage_core::storage_methods::{SearchMethods, StorageMethods};
use crate::{decode_str, decode_u128};

use crate::helix_engine::types::GraphError;
use crate::protocol::{
    items::{Edge, Node},
    value::Value,
};

use super::storage_methods::{BasicStorageMethods, DBMethods};

// Database names for different stores
const DB_NODES: &str = "nodes"; // For node data (n:)
const DB_EDGES: &str = "edges"; // For edge data (e:)
const DB_NODE_LABELS: &str = "node_labels"; // For node label indices (nl:)
const DB_EDGE_LABELS: &str = "edge_labels"; // For edge label indices (el:)
const DB_OUT_EDGES: &str = "out_edges"; // For outgoing edge indices (o:)
const DB_IN_EDGES: &str = "in_edges"; // For incoming edge indices (i:)

// Key prefixes for different types of data
pub const NODE_PREFIX: &[u8] = b"n:";
pub const EDGE_PREFIX: &[u8] = b"e";
const NODE_LABEL_PREFIX: &[u8] = b"nl";
const EDGE_LABEL_PREFIX: &[u8] = b"el";
pub const OUT_EDGES_PREFIX: &[u8] = b"o";
pub const IN_EDGES_PREFIX: &[u8] = b"i";

pub struct HelixGraphStorage {
    pub graph_env: Env<WithTls>,
    pub nodes_db: Database<Bytes, Bytes>,
    pub edges_db: Database<Bytes, Bytes>,
    pub node_labels_db: Database<Bytes, Unit>,
    pub edge_labels_db: Database<Bytes, Unit>,
    pub out_edges_db: Database<Bytes, Bytes>,
    pub in_edges_db: Database<Bytes, Bytes>,
    pub secondary_indices: HashMap<String, Database<Bytes, Bytes>>,
    pub vectors: VectorCore,
}

impl HelixGraphStorage {
    pub fn new(path: &str, config: Config) -> Result<HelixGraphStorage, GraphError> {
        fs::create_dir_all(path)?;

        // Configure and open LMDB environment
        let graph_env = unsafe {
            EnvOpenOptions::new()
                .map_size(config.vector_config.db_max_size.unwrap_or(30) * 1024 * 1024 * 1024) // 10GB max
                .max_dbs(20)
                .max_readers(200)
                .open(Path::new(path))?
        };

        let mut wtxn = graph_env.write_txn()?;

        // Create/open all necessary databases
        let nodes_db = graph_env.create_database(&mut wtxn, Some(DB_NODES))?;
        let edges_db = graph_env.create_database(&mut wtxn, Some(DB_EDGES))?;
        let node_labels_db = graph_env.create_database(&mut wtxn, Some(DB_NODE_LABELS))?;
        let edge_labels_db = graph_env.create_database(&mut wtxn, Some(DB_EDGE_LABELS))?;
        let out_edges_db = graph_env.create_database(&mut wtxn, Some(DB_OUT_EDGES))?;
        let in_edges_db = graph_env.create_database(&mut wtxn, Some(DB_IN_EDGES))?;
        // Create secondary indices
        let mut secondary_indices = HashMap::new();
        if let Some(indexes) = config.graph_config.secondary_indices {
            for index in indexes {
                secondary_indices.insert(
                    index.clone(),
                    graph_env.create_database(&mut wtxn, Some(&index))?,
                );
            }
        }
        println!("Secondary Indices: {:?}", secondary_indices);

        let vectors = VectorCore::new(
            &graph_env,
            &mut wtxn,
            HNSWConfig::new(
                config.vector_config.m,
                config.vector_config.ef_construction,
                config.vector_config.ef_search,
            ),
        )?;

        wtxn.commit()?;
        Ok(Self {
            graph_env,
            nodes_db,
            edges_db,
            node_labels_db,
            edge_labels_db,
            out_edges_db,
            in_edges_db,
            secondary_indices,
            vectors,
        })
    }

    pub fn get_u128_from_bytes(bytes: &[u8]) -> Result<u128, GraphError> {
        let mut arr = [0u8; 16];
        arr.copy_from_slice(bytes);
        let res = u128::from_le_bytes(arr);
        Ok(res)
    }

    #[inline(always)]
    pub fn new_node(label: &str, properties: impl IntoIterator<Item = (String, Value)>) -> Node {
        Node {
            id: Uuid::new_v4().as_u128(),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
        }
    }

    #[inline(always)]
    pub fn new_edge(
        label: &str,
        from_node: u128,
        to_node: u128,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Edge {
        Edge {
            id: Uuid::new_v4().as_u128(),
            label: label.to_string(),
            from_node,
            to_node,
            properties: HashMap::from_iter(properties),
        }
    }

    pub fn get_random_node(&self, txn: &RoTxn) -> Result<Node, GraphError> {
        match self.nodes_db.first(&txn)? {
            Some((_, data)) => Ok(bincode::deserialize(data)?),
            None => Err(GraphError::NodeNotFound),
        }
    }

    #[inline(always)]
    pub fn node_key(id: &u128) -> Vec<u8> {
        [NODE_PREFIX, &id.to_le_bytes()].concat()
    }

    #[inline(always)]
    pub fn edge_key(id: &u128) -> Vec<u8> {
        [EDGE_PREFIX, &id.to_le_bytes()].concat()
    }

    #[inline(always)]
    pub fn node_label_key(label: &str, id: Option<&u128>) -> Vec<u8> {
        match id {
            Some(id) => [NODE_LABEL_PREFIX, label.as_bytes(), &id.to_le_bytes()].concat(),
            None => [NODE_LABEL_PREFIX, label.as_bytes()].concat(),
        }
    }

    #[inline(always)]
    pub fn edge_label_key(label: &str, id: Option<&u128>) -> Vec<u8> {
        match id {
            Some(id) => [EDGE_LABEL_PREFIX, label.as_bytes(), &id.to_le_bytes()].concat(),
            None => [EDGE_LABEL_PREFIX, label.as_bytes()].concat(),
        }
    }

    #[inline(always)]
    pub fn out_edge_key(from_node_id: &u128, label: &str, to_node_id: Option<&u128>) -> Vec<u8> {
        match to_node_id {
            Some(to_node_id) => [
                OUT_EDGES_PREFIX,
                &from_node_id.to_le_bytes(),
                label.as_bytes(),
                &to_node_id.to_le_bytes(),
            ]
            .concat(),
            None => [
                OUT_EDGES_PREFIX,
                &from_node_id.to_le_bytes(),
                label.as_bytes(),
            ]
            .concat(),
        }
    }

    #[inline(always)]
    pub fn in_edge_key(to_node_id: &u128, label: &str, from_node_id: Option<&u128>) -> Vec<u8> {
        match from_node_id {
            Some(from_node_id) => [
                IN_EDGES_PREFIX,
                &to_node_id.to_le_bytes(),
                label.as_bytes(),
                &from_node_id.to_le_bytes(),
            ]
            .concat(),
            None => [IN_EDGES_PREFIX, &to_node_id.to_le_bytes(), label.as_bytes()].concat(),
        }
    }

    pub fn create_node_(
        &self,
        txn: &mut RwTxn,
        label: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
        secondary_indices: Option<&[String]>,
        id: Option<u128>,
    ) -> Result<(), GraphError> {
        let node = Node {
            id: id.unwrap_or(Uuid::new_v4().as_u128()),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
        };

        // Store node data
        self.nodes_db
            .put(txn, &Self::node_key(&node.id), &bincode::serialize(&node)?)?;

        // Store node label index
        self.node_labels_db
            .put(txn, &Self::node_label_key(&node.label, Some(&node.id)), &())?;

        for index in secondary_indices.unwrap_or(&[]) {
            match self.secondary_indices.get(index) {
                Some(db) => {
                    let key = match node.check_property(index) {
                        Some(value) => value,
                        None => {
                            return Err(GraphError::New(format!(
                                "Secondary Index {} not found",
                                index
                            )))
                        }
                    };
                    db.put(txn, &bincode::serialize(&key)?, &node.id.to_le_bytes())?;
                }
                None => {
                    return Err(GraphError::New(format!(
                        "Secondary Index {} not found",
                        index
                    )))
                }
            }
        }
        Ok(())
    }

    pub fn create_edge_(
        &self,
        txn: &mut RwTxn,
        label: &str,
        from_node: u128,
        to_node: u128,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<(), GraphError> {
        // Check if nodes exist

        // if self.check_exists(from_node)? || self.check_exists(to_node)? {
        //     return Err(GraphError::New(
        //         "One or both nodes do not exist".to_string(),
        //     ));
        // }
        if self
            .nodes_db
            .get(txn, Self::node_key(&from_node).as_slice())?
            .is_none()
            || self
                .nodes_db
                .get(txn, Self::node_key(&to_node).as_slice())?
                .is_none()
        {
            return Err(GraphError::NodeNotFound);
        }

        let edge = Edge {
            id: Uuid::new_v4().as_u128(),
            label: label.to_string(),
            from_node: from_node,
            to_node: to_node,
            properties: HashMap::from_iter(properties),
        };

        // Store edge data
        self.edges_db
            .put(txn, &Self::edge_key(&edge.id), &bincode::serialize(&edge)?)?;

        // Store edge label index
        self.edge_labels_db
            .put(txn, &Self::edge_label_key(label, Some(&edge.id)), &())?;

        // Store edge - node maps
        self.out_edges_db.put(
            txn,
            &Self::out_edge_key(&from_node, label, Some(&to_node)),
            &edge.id.to_le_bytes(),
        )?;

        self.in_edges_db.put(
            txn,
            &Self::in_edge_key(&to_node, label, Some(&from_node)),
            &edge.id.to_le_bytes(),
        )?;

        Ok(())
    }
}

impl DBMethods for HelixGraphStorage {
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
            .ok_or(GraphError::New(format!(
                "Secondary Index {} not found",
                name
            )))?;
        db.clear(&mut wtxn)?;
        wtxn.commit()?;
        self.secondary_indices.remove(name);
        Ok(())
    }
}

impl BasicStorageMethods for HelixGraphStorage {
    #[inline(always)]
    fn get_temp_node<'a>(&self, txn: &'a RoTxn, id: &u128) -> Result<&'a [u8], GraphError> {
        match self.nodes_db.get(&txn, Self::node_key(id).as_slice())? {
            Some(data) => Ok(data),
            None => Err(GraphError::NodeNotFound),
        }
    }

    #[inline(always)]
    fn get_temp_edge<'a>(&self, txn: &'a RoTxn, id: &u128) -> Result<&'a [u8], GraphError> {
        match self.edges_db.get(&txn, Self::edge_key(id).as_slice())? {
            Some(data) => Ok(data),
            None => Err(GraphError::EdgeNotFound),
        }
    }
}

impl StorageMethods for HelixGraphStorage {
    #[inline(always)]
    fn check_exists(&self, txn: &RoTxn, id: &u128) -> Result<bool, GraphError> {
        // let txn = txn.read_txn();
        let exists = self
            .nodes_db
            .get(txn, Self::node_key(id).as_slice())?
            .is_some();
        Ok(exists)
    }

    #[inline(always)]
    fn get_node(&self, txn: &RoTxn, id: &u128) -> Result<Node, GraphError> {
        let node = match self
            .nodes_db
            .get(txn, Self::node_key(id).as_slice())?
        {
            Some(data) => data,
            None => return Err(GraphError::NodeNotFound),
        };
        let node: Node = match SerializedNode::decode_node(&node, *id) {
            Ok(node) => node,
            Err(e) => return Err(e),
        };
        Ok(node)
    }

    #[inline(always)]
    fn get_edge(&self, txn: &RoTxn, id: &u128) -> Result<Edge, GraphError> {
        let edge = match self
            .edges_db
            .get(txn, Self::edge_key(id).as_slice())?
        {
            Some(data) => data,
            None => return Err(GraphError::EdgeNotFound),
        };
        let edge: Edge = match SerializedEdge::decode_edge(&edge, *id) {
            Ok(edge) => edge,
            Err(e) => return Err(e),
        };
        Ok(edge)
    }

    fn get_node_by_secondary_index(
        &self,
        txn: &RoTxn,
        index: &str,
        key: &Value,
    ) -> Result<Node, GraphError> {
        let db = self
            .secondary_indices
            .get(index)
            .ok_or(GraphError::New(format!(
                "Secondary Index {} not found",
                index
            )))?;
        let node_id = db
            .get(txn, &bincode::serialize(key)?)?
            .ok_or(GraphError::NodeNotFound)?;
        let node_id = Self::get_u128_from_bytes(&node_id)?;
        self.get_node(txn, &node_id)
    }

    fn get_out_edges(
        &self,
        txn: &RoTxn,
        node_id: &u128,
        edge_label: &str,
    ) -> Result<Vec<Edge>, GraphError> {
        let mut edges = Vec::with_capacity(512);

        let prefix = Self::out_edge_key(node_id, edge_label, None);
        let iter = self
            .out_edges_db
            .lazily_decode_data()
            .prefix_iter(&txn, &prefix)?;

        for result in iter {
            let (_, value) = result?;
            let edge_id = decode_u128!(value);

            let edge = self.get_edge(&txn, &edge_id)?;
            if edge_label.is_empty() || edge.label == edge_label {
                edges.push(edge);
            }
        }

        Ok(edges)
    }

    fn get_in_edges(
        &self,
        txn: &RoTxn,
        node_id: &u128,
        edge_label: &str,
    ) -> Result<Vec<Edge>, GraphError> {
        let mut edges = Vec::with_capacity(512);

        let prefix = Self::in_edge_key(node_id, edge_label, None);
        let iter = self
            .in_edges_db
            .lazily_decode_data()
            .prefix_iter(&txn, &prefix)?;

        for result in iter {
            let (_, value) = result?;
            let edge_id = decode_u128!(value);

            let edge = self.get_edge(&txn, &edge_id)?;
            if edge_label.is_empty() || edge.label == edge_label {
                edges.push(edge);
            }
        }

        Ok(edges)
    }

    fn get_out_nodes(
        &self,
        txn: &RoTxn,
        node_id: &u128,
        edge_label: &str,
    ) -> Result<Vec<Node>, GraphError> {
        let mut nodes = Vec::with_capacity(512);
        let prefix = Self::out_edge_key(node_id, edge_label, None);
        let iter = self
            .out_edges_db
            .lazily_decode_data()
            .prefix_iter(txn, &prefix)?;

        for result in iter {
            let (_, value) = result?;
            let edge_id = decode_u128!(value);

            match self.get_edge(txn, &edge_id) {
                Ok(edge) => {
                    if let Ok(node) = self.get_node(txn, &edge.to_node) {
                        nodes.push(node);
                    }
                }
                Err(e) => {
                    println!("error: {:?}", e);
                }
            }
        }
        // println!("nodes: {:?}", nodes.len());

        Ok(nodes)
    }

    fn get_in_nodes(
        &self,
        txn: &RoTxn,
        node_id: &u128,
        edge_label: &str,
    ) -> Result<Vec<Node>, GraphError> {
        let mut nodes = Vec::with_capacity(512);
        let prefix = Self::in_edge_key(node_id, edge_label, None);
        let iter = self
            .in_edges_db
            .lazily_decode_data()
            .prefix_iter(txn, &prefix)?;

        for result in iter {
            let (_, value) = result?;
            let edge_id = decode_u128!(value);
            match self.get_edge(txn, &edge_id) {
                Ok(edge) => {
                    if let Ok(node) = self.get_node(txn, &edge.from_node) {
                        nodes.push(node);
                    }
                }
                Err(e) => {
                    println!("error: {:?}", e);
                }
            }
        }

        Ok(nodes)
    }

    fn get_all_nodes(&self, txn: &RoTxn) -> Result<Vec<Node>, GraphError> {
        let mut nodes = Vec::with_capacity(self.nodes_db.len(txn)? as usize);

        let iter = self.nodes_db.iter(&txn)?;

        for result in iter {
            let (_, value) = result?;
            if !value.is_empty() {
                let node: Node = bincode::deserialize(value)?;
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    fn get_nodes_by_types(&self, txn: &RoTxn, types: &[&str]) -> Result<Vec<Node>, GraphError> {
        let mut nodes = Vec::new();

        for label in types {
            let prefix = [NODE_LABEL_PREFIX, label.as_bytes()].concat();
            let iter = self
                .node_labels_db
                .lazily_decode_data()
                .prefix_iter(&txn, &prefix)?;

            for result in iter {
                let (key, _) = result?;
                let node_id = Self::get_u128_from_bytes(&key[prefix.len()..])?;

                let n: Result<Node, GraphError> =
                    match self.nodes_db.get(&txn, &Self::node_key(&node_id))? {
                        Some(data) => Ok(bincode::deserialize(data)?),
                        None => Err(GraphError::NodeNotFound),
                    };
                if let Ok(node) = n {
                    nodes.push(node);
                }
            }
        }

        Ok(nodes)
    }

    fn get_all_edges(&self, txn: &RoTxn) -> Result<Vec<Edge>, GraphError> {
        let mut edges = Vec::with_capacity(self.edges_db.len(txn)? as usize);

        let iter = self.edges_db.iter(&txn)?;

        for result in iter {
            let (_, value) = result?;
            if !value.is_empty() {
                let edge: Edge = bincode::deserialize(value)?;
                edges.push(edge);
            }
        }

        Ok(edges)
    }

    fn create_node(
        &self,
        txn: &mut RwTxn,
        label: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
        secondary_indices: Option<&[String]>,
        id: Option<u128>,
    ) -> Result<Node, GraphError> {
        let node = Node {
            id: id.unwrap_or(Uuid::new_v4().as_u128()),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
        };

        // Store node data
        self.nodes_db
            .put(txn, &Self::node_key(&node.id), &bincode::serialize(&node)?)?;

        // Store node label index
        self.node_labels_db
            .put(txn, &Self::node_label_key(&node.label, Some(&node.id)), &())?;

        for index in secondary_indices.unwrap_or(&[]) {
            match self.secondary_indices.get(index) {
                Some(db) => {
                    let key = match node.check_property(index) {
                        Some(value) => value,
                        None => {
                            return Err(GraphError::New(format!(
                                "Secondary Index {} not found",
                                index
                            )))
                        }
                    };
                    db.put(txn, &bincode::serialize(&key)?, &node.id.to_le_bytes())?;
                }
                None => {
                    return Err(GraphError::New(format!(
                        "Secondary Index {} not found",
                        index
                    )))
                }
            }
        }

        Ok(node)
    }

    fn create_edge(
        &self,
        txn: &mut RwTxn,
        label: &str,
        from_node: &u128,
        to_node: &u128,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Edge, GraphError> {
        // Check if nodes exist

        // if self.check_exists(from_node)? || self.check_exists(to_node)? {
        //     return Err(GraphError::New(
        //         "One or both nodes do not exist".to_string(),
        //     ));
        // }
        if self
            .nodes_db
            .get(txn, Self::node_key(from_node).as_slice())?
            .is_none()
            || self
                .nodes_db
                .get(txn, Self::node_key(to_node).as_slice())?
                .is_none()
        {
            return Err(GraphError::NodeNotFound);
        }

        let edge = Edge {
            id: Uuid::new_v4().as_u128(),
            label: label.to_string(),
            from_node: *from_node,
            to_node: *to_node,
            properties: HashMap::from_iter(properties),
        };

        // Store edge data
        self.edges_db
            .put(txn, &Self::edge_key(&edge.id), &bincode::serialize(&edge)?)?;

        // Store edge label index
        self.edge_labels_db
            .put(txn, &Self::edge_label_key(label, Some(&edge.id)), &())?;

        // Store edge - node maps
        self.out_edges_db.put(
            txn,
            &Self::out_edge_key(from_node, label, Some(to_node)),
            &edge.id.to_le_bytes(),
        )?;

        self.in_edges_db.put(
            txn,
            &Self::in_edge_key(to_node, label, Some(from_node)),
            &edge.id.to_le_bytes(),
        )?;

        Ok(edge)
    }

    fn drop_node(&self, txn: &mut RwTxn, id: &u128) -> Result<(), GraphError> {
        // Get node to get its label
        let node = self.get_node(txn, id)?;

        // Delete outgoing edges
        let out_prefix = Self::out_edge_key(id, "", None);
        let out_edges = {
            let iter = self
                .out_edges_db
                .lazily_decode_data()
                .prefix_iter(&txn, &out_prefix)?;

            let capacity = match iter.size_hint() {
                (_, Some(upper)) => upper,
                (lower, None) => lower,
            };
            let mut out_edges = Vec::with_capacity(capacity);

            for result in iter {
                let (_, value) = result?;
                let edge_id = decode_u128!(value);

                if let Some(edge_data) = &self.edges_db.get(&txn, &Self::edge_key(&edge_id))? {
                    let edge: Edge = bincode::deserialize(edge_data)?;
                    out_edges.push(edge);
                }
            }
            out_edges
        };

        // Delete incoming edges
        let in_prefix = Self::in_edge_key(id, "", None);
        let in_edges = {
            let iter = self
                .in_edges_db
                .lazily_decode_data()
                .prefix_iter(&txn, &in_prefix)?;
            let capacity = match iter.size_hint() {
                (_, Some(c)) => c,
                (c, None) => c,
            };
            let mut in_edges = Vec::with_capacity(capacity);

            for result in iter {
                let (_, value) = result?;
                let edge_id = decode_u128!(value);

                if let Some(edge_data) = self.edges_db.get(&txn, &Self::edge_key(&edge_id))? {
                    let edge: Edge = bincode::deserialize(edge_data)?;
                    in_edges.push(edge);
                }
            }
            in_edges
        };

        // Delete all related data
        for edge in out_edges.iter().chain(in_edges.iter()) {
            // Delete edge data
            self.edges_db.delete(txn, &Self::edge_key(&edge.id))?;

            self.edge_labels_db
                .delete(txn, &Self::edge_label_key(&edge.label, Some(&edge.id)))?;
            self.out_edges_db.delete(
                txn,
                &Self::out_edge_key(&edge.from_node, &edge.label, Some(&edge.to_node)),
            )?;
            self.in_edges_db.delete(
                txn,
                &Self::in_edge_key(&edge.to_node, &edge.label, Some(&edge.from_node)),
            )?;
        }

        // Delete node data and label
        self.nodes_db.delete(txn, Self::node_key(id).as_slice())?;
        self.node_labels_db
            .delete(txn, &Self::node_label_key(&node.label, Some(&node.id)))?;

        Ok(())
    }

    fn drop_edge(&self, txn: &mut RwTxn, edge_id: &u128) -> Result<(), GraphError> {
        // Get edge data first
        let edge_data = match self.edges_db.get(&txn, &Self::edge_key(edge_id))? {
            Some(data) => data,
            None => return Err(GraphError::EdgeNotFound),
        };
        let edge: Edge = bincode::deserialize(edge_data)?;

        // Delete all edge-related data
        self.edges_db.delete(txn, &Self::edge_key(edge_id))?;
        self.edge_labels_db
            .delete(txn, &Self::edge_label_key(&edge.label, Some(&edge.id)))?;
        self.out_edges_db.delete(
            txn,
            &Self::out_edge_key(&edge.from_node, &edge.label, Some(&edge.to_node)),
        )?;
        self.in_edges_db.delete(
            txn,
            &Self::in_edge_key(&edge.to_node, &edge.label, Some(&edge.from_node)),
        )?;

        Ok(())
    }

    fn update_node(
        &self,
        txn: &mut RwTxn,
        id: &u128,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Node, GraphError> {
        let mut node = self.get_node(txn, id)?;
        properties.into_iter().for_each(|(k, v)| {
            node.properties.insert(k, v);
        });
        for (key, v) in node.properties.iter() {
            if let Some(db) = self.secondary_indices.get(key) {
                // println!("Updating secondary index: {}, {}", key, v);
                db.put(txn, &bincode::serialize(v)?, &node.id.to_le_bytes())?;
            }
        }
        self.nodes_db
            .put(txn, &Self::node_key(id), &bincode::serialize(&node)?)?;

        Ok(node)
    }

    fn update_edge(
        &self,
        txn: &mut RwTxn,
        id: &u128,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Edge, GraphError> {
        let mut edge = self.get_edge(txn, id)?;
        properties.into_iter().for_each(|(k, v)| {
            edge.properties.insert(k, v);
        });
        self.edges_db
            .put(txn, &Self::edge_key(id), &bincode::serialize(&edge)?)?;
        Ok(edge)
    }
}

impl SearchMethods for HelixGraphStorage {
    fn shortest_path(
        &self,
        txn: &RoTxn,
        edge_label: &str,
        from_id: &u128,
        to_id: &u128,
    ) -> Result<(Vec<Node>, Vec<Edge>), GraphError> {
        let mut queue = VecDeque::with_capacity(32);
        let mut visited = HashSet::with_capacity(64);
        let mut parent: HashMap<u128, (u128, Edge)> = HashMap::with_capacity(32);
        queue.push_back(*from_id);
        visited.insert(*from_id);

        let reconstruct_path = |parent: &HashMap<u128, (u128, Edge)>,
                                start_id: &u128,
                                end_id: &u128|
         -> Result<(Vec<Node>, Vec<Edge>), GraphError> {
            let mut nodes = Vec::with_capacity(parent.len());
            let mut edges = Vec::with_capacity(parent.len() - 1);

            let mut current = end_id;

            while current != start_id {
                nodes.push(self.get_node(txn, current)?);

                let (prev_node, edge) = &parent[current];
                edges.push(edge.clone());
                current = prev_node;
            }

            nodes.push(self.get_node(txn, start_id)?);

            Ok((nodes, edges))
        };

        while let Some(current_id) = queue.pop_front() {
            let out_prefix = Self::out_edge_key(&current_id, edge_label, None);
            let iter = self
                .out_edges_db
                .lazily_decode_data()
                .prefix_iter(&txn, &out_prefix)?;

            for result in iter {
                let (key, value) = result?;
                let to_node = Self::get_u128_from_bytes(&key[out_prefix.len()..])?;

                if !visited.contains(&to_node) {
                    visited.insert(to_node);
                    let edge_id = decode_u128!(value);
                    let edge = self.get_edge(&txn, &edge_id)?;
                    parent.insert(to_node, (current_id, edge));

                    if to_node == *to_id {
                        return reconstruct_path(&parent, from_id, to_id);
                    }

                    queue.push_back(to_node);
                }
            }
        }

        Err(GraphError::from(format!(
            "No path found between {} and {}",
            from_id, to_id
        )))
    }

    fn shortest_mutual_path(
        &self,
        txn: &RoTxn,
        edge_label: &str,
        from_id: &u128,
        to_id: &u128,
    ) -> Result<(Vec<Node>, Vec<Edge>), GraphError> {
        let mut queue = VecDeque::with_capacity(32);
        let mut visited = HashSet::with_capacity(64);
        let mut parent = HashMap::with_capacity(32);

        queue.push_back(*from_id);
        visited.insert(*from_id);

        let reconstruct_path = |parent: &HashMap<u128, (u128, Edge)>,
                                start_id: &u128,
                                end_id: &u128|
         -> Result<(Vec<Node>, Vec<Edge>), GraphError> {
            let mut nodes = Vec::with_capacity(parent.len());
            let mut edges = Vec::with_capacity(parent.len() - 1);

            let mut current = end_id;

            while current != start_id {
                nodes.push(self.get_node(txn, current)?);

                let (prev_node, edge) = &parent[current];
                edges.push(edge.clone());
                current = prev_node;
            }
            nodes.push(self.get_node(txn, start_id)?);
            Ok((nodes, edges))
        };

        while let Some(current_id) = queue.pop_front() {
            let out_prefix = Self::out_edge_key(&current_id, edge_label, None);
            let iter = self
                .out_edges_db
                .lazily_decode_data()
                .prefix_iter(&txn, &out_prefix)?;

            for result in iter {
                let (key, value) = result?;
                let to_node = Self::get_u128_from_bytes(&key[out_prefix.len()..])?;

                println!("To Node: {}", to_node);
                println!("Current: {}", current_id);
                // Check if there's a reverse edge
                let reverse_edge_key = Self::out_edge_key(&to_node, edge_label, Some(&current_id));

                let has_reverse_edge = self.out_edges_db.get(&txn, &reverse_edge_key)?.is_some();

                // Only proceed if there's a mutual connection
                if has_reverse_edge && !visited.contains(&to_node) {
                    visited.insert(to_node);
                    let edge_id = decode_u128!(value);
                    let edge = self.get_edge(&txn, &edge_id)?;
                    parent.insert(to_node, (current_id, edge));

                    if to_node == *to_id {
                        return reconstruct_path(&parent, from_id, to_id);
                    }

                    queue.push_back(to_node);
                }
            }
        }

        Err(GraphError::from(format!(
            "No mutual path found between {} and {}",
            from_id, to_id
        )))
    }
}
