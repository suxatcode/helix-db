use bincode::{deserialize, serialize};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompactionStyle, DBCompressionType,
    IteratorMode, Options, ReadOptions, WriteBatch, WriteBatchWithTransaction, WriteOptions, DB,
};

use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Deref;

use uuid::Uuid;

use crate::storage_core::storage_methods::{SearchMethods, StorageMethods};
use crate::types::GraphError;
use protocol::{value::Value, Edge, Node};
use rayon::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const CF_NODES: &str = "nodes"; // For node data (n:)
const CF_EDGES: &str = "edges"; // For edge data (e:)
const CF_INDICES: &str = "indices"; // For all indices (nl:, el:, o:, i:)

// Byte values of data-type key prefixes
const NODE_PREFIX: &[u8] = b"n:";
const EDGE_PREFIX: &[u8] = b"e:";
const NODE_LABEL_PREFIX: &[u8] = b"nl:";
const EDGE_LABEL_PREFIX: &[u8] = b"el:";
const OUT_EDGES_PREFIX: &[u8] = b"o:";
const IN_EDGES_PREFIX: &[u8] = b"i:";

const RAH_SMALL: usize = 2 * 1024 * 1024;
const RAH_MEDIUM: usize = 4 * 1024 * 1024;
const RAH_LARGE: usize = 8 * 1024 * 1024;
const RAH_XLARGE: usize = 24 * 1024 * 1024;

pub struct HelixGraphStorage {
    db: DB,
}

impl HelixGraphStorage {
    /// HelixGraphStorage struct constructor
    pub fn new(path: &str) -> Result<HelixGraphStorage, GraphError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let mut opts = Options::default();

        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.increase_parallelism(num_cpus::get() as i32);
        opts.set_max_background_jobs(8);
        // opts.set_compaction_style(DBCompactionStyle::);

        // Write path optimizations
        opts.set_write_buffer_size(256 * 1024 * 1024); // 256MB write buffer
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_level_zero_slowdown_writes_trigger(20);
        opts.set_level_zero_stop_writes_trigger(36);

        // Configure compaction
        opts.set_disable_auto_compactions(false);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB
        opts.set_target_file_size_multiplier(1);
        opts.set_max_bytes_for_level_base(512 * 1024 * 1024); // 512MB
        opts.set_max_bytes_for_level_multiplier(8.0);

        opts.set_compaction_style(DBCompactionStyle::Level);

        // Optimize level-based compaction
        opts.set_level_compaction_dynamic_level_bytes(true);

        // Increase read performance at cost of space
        opts.set_optimize_filters_for_hits(true);
        opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(8));

        // Setup column families with specific options
        let mut node_opts = Options::default();
        let mut edge_opts = Options::default();
        let mut index_opts = Options::default();

        // Node CF optimizations
        let node_cache = Cache::new_lru_cache(1 * 1024 * 1024 * 1024); // 4GB cache
        let mut node_block_opts = BlockBasedOptions::default();
        node_block_opts.set_block_cache(&node_cache);
        node_block_opts.set_block_size(32 * 1024); // 32KB blocks
        node_block_opts.set_cache_index_and_filter_blocks(true);
        node_block_opts.set_bloom_filter(10.0, false);
        node_opts.set_block_based_table_factory(&node_block_opts);

        // Edge CF optimizations
        let edge_cache = Cache::new_lru_cache(2 * 1024 * 1024 * 1024); // 8GB cache
        let mut edge_block_opts = BlockBasedOptions::default();
        edge_block_opts.set_block_cache(&edge_cache);
        edge_block_opts.set_block_size(64 * 1024); // 64KB blocks
        edge_block_opts.set_cache_index_and_filter_blocks(true);
        edge_block_opts.set_bloom_filter(10.0, false);
        edge_opts.set_block_based_table_factory(&edge_block_opts);

        // Index CF optimizations (for edge indices)
        let index_cache = Cache::new_lru_cache(1 * 1024 * 1024 * 1024); // 2GB cache
        let mut index_block_opts = BlockBasedOptions::default();
        index_block_opts.set_block_cache(&index_cache);
        index_block_opts.set_block_size(16 * 1024); // 16KB blocks
        index_block_opts.set_cache_index_and_filter_blocks(true);
        index_block_opts.set_bloom_filter(10.0, false);
        index_opts.set_block_based_table_factory(&index_block_opts);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_NODES, node_opts),
            ColumnFamilyDescriptor::new(CF_EDGES, edge_opts),
            ColumnFamilyDescriptor::new(CF_INDICES, index_opts),
        ];

        let db = match DB::open_cf_descriptors(&opts, path, cf_descriptors) {
            Ok(db) => db,
            Err(err) => return Err(GraphError::from(err)),
        };

        let cf_edges = db
            .cf_handle(CF_EDGES)
            .ok_or_else(|| GraphError::from("Column Family not found"))?;
        db.set_options_cf(
            &cf_edges,
            &[
                ("level0_file_num_compaction_trigger", "2"),
                ("level0_slowdown_writes_trigger", "20"),
                ("level0_stop_writes_trigger", "36"),
                ("target_file_size_base", "67108864"), // 64MB
                ("max_bytes_for_level_base", "536870912"), // 512MB
                ("write_buffer_size", "67108864"),     // 64MB
                ("max_write_buffer_number", "2"),
            ],
        )?;

        drop(cf_edges);
        Ok(Self { db })
    }
    #[inline]
    fn get_optimized_read_options(rah_size: usize) -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_verify_checksums(false);
        opts.set_readahead_size(rah_size);
        opts.set_prefix_same_as_start(true);
        opts.set_async_io(true);
        opts.set_tailing(true);
        opts.fill_cache(false);
        opts.set_pin_data(true); // Add this to prevent copying
        opts.set_background_purge_on_iterator_cleanup(true); // Add this
        opts
    }

    /// Creates node key using the prefix and given id
    #[inline(always)]
    pub fn node_key(id: &str) -> Vec<u8> {
        [NODE_PREFIX, id.as_bytes()].concat()
    }

    /// Creates edge key using the prefix and given id
    #[inline(always)]
    pub fn edge_key(id: &str) -> Vec<u8> {
        [EDGE_PREFIX, id.as_bytes()].concat()
    }

    /// Creates node label key using the prefix, the given label, and id
    #[inline(always)]
    pub fn node_label_key(label: &str, id: &str) -> Vec<u8> {
        [NODE_LABEL_PREFIX, label.as_bytes(), b":", id.as_bytes()].concat()
    }

    /// Creates edge label key using the prefix, the given label, and  id
    #[inline(always)]
    pub fn edge_label_key(label: &str, id: &str) -> Vec<u8> {
        [EDGE_LABEL_PREFIX, label.as_bytes(), b":", id.as_bytes()].concat()
    }

    /// Creates key for an outgoing edge using the prefix, source node id, and edge id
    /// 75 Bytes with edge id, 39 bytes without edge id
    #[inline(always)]
    pub fn out_edge_key(source_node_id: &str, edge_id: &str) -> Vec<u8> {
        [
            OUT_EDGES_PREFIX,
            source_node_id.as_bytes(),
            b":",
            edge_id.as_bytes(),
        ]
        .concat()
    }
    
    /// Creates key for an incoming edge using the prefix, sink node id, and edge id
    /// 75 Bytes with edge id, 39 bytes without edge id
    #[inline(always)]
    pub fn in_edge_key(sink_node_id: &str, edge_id: &str) -> Vec<u8> {
        [
            IN_EDGES_PREFIX,
            sink_node_id.as_bytes(),
            b":",
            edge_id.as_bytes(),
        ]
        .concat()
    }
}

impl StorageMethods for HelixGraphStorage {
    #[inline]
    fn check_exists(&self, id: &str) -> Result<bool, GraphError> {
        let cf_nodes = self
            .db
            .cf_handle(CF_NODES)
            .ok_or(GraphError::from("Column Family not found"))?;
        match self
            .db
            .get_pinned_cf(&cf_nodes, [NODE_PREFIX, id.as_bytes()].concat())
        {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(err) => Err(GraphError::from(err)),
        }
    }
    #[inline]
    fn get_temp_node(&self, id: &str) -> Result<Node, GraphError> {
        let cf_nodes = self
            .db
            .cf_handle(CF_NODES)
            .ok_or(GraphError::from("Column Family not found"))?;
        match self
            .db
            .get_pinned_cf(&cf_nodes, [NODE_PREFIX, id.as_bytes()].concat())
        {
            Ok(Some(data)) => Ok(deserialize(&data).unwrap()),
            Ok(None) => Err(GraphError::New(format!("Node not found: {}", id))),
            Err(err) => Err(GraphError::from(err)),
        }
    }
    #[inline]
    fn get_temp_edge(&self, id: &str) -> Result<Edge, GraphError> {
        let cf_edges = self
            .db
            .cf_handle(CF_EDGES)
            .ok_or(GraphError::from("Column Family not found"))?;
        match self
            .db
            .get_pinned_cf(&cf_edges, [EDGE_PREFIX, id.as_bytes()].concat())
        {
            Ok(Some(data)) => Ok(deserialize(&data).unwrap()),
            Ok(None) => Err(GraphError::New(format!("Edge not found: {}", id))),
            Err(err) => Err(GraphError::from(err)),
        }
    }

    #[inline]
    fn get_node(&self, id: &str) -> Result<Node, GraphError> {
        let cf_nodes = self
            .db
            .cf_handle(CF_NODES)
            .ok_or(GraphError::from("Column Family not found"))?;
        match self
            .db
            .get_cf(&cf_nodes, [NODE_PREFIX, id.as_bytes()].concat())
        {
            Ok(Some(data)) => deserialize::<Node>(&data).map_err(GraphError::from),
            Ok(None) => Err(GraphError::New(format!("Item not found: {}", id))),
            Err(err) => Err(GraphError::from(err)),
        }
    }
    #[inline]
    fn get_edge(&self, id: &str) -> Result<Edge, GraphError> {
        let cf_edges = self
            .db
            .cf_handle(CF_EDGES)
            .ok_or(GraphError::from("Column Family not found"))?;
        match self
            .db
            .get_cf(&cf_edges, [EDGE_PREFIX, id.as_bytes()].concat())
        {
            Ok(Some(data)) => deserialize::<Edge>(&data).map_err(GraphError::from),
            Ok(None) => Err(GraphError::New(format!("Item not found: {}", id))),
            Err(err) => Err(GraphError::from(err)),
        }
    }

    fn get_out_edges(&self, node_id: &str, edge_label: &str) -> Result<Vec<Edge>, GraphError> {
        let cf_edge_index = self
            .db
            .cf_handle(CF_INDICES)
            .ok_or(GraphError::from("Column Family not found"))?;

        let mut edges = Vec::new();

        let read_opts = Self::get_optimized_read_options(RAH_MEDIUM);

        let out_prefix = Self::out_edge_key(node_id, "");
        let iter = self.db.iterator_cf_opt(
            &cf_edge_index,
            read_opts,
            IteratorMode::From(&out_prefix, rocksdb::Direction::Forward),
        );

        // get edge values
        match edge_label {
            "" => {
                for result in iter {
                    let (key, _) = result?;
                    if !key.starts_with(&out_prefix) {
                        break;
                    }

                    let edge_id = String::from_utf8(key[out_prefix.len()..].to_vec())?;
                    let edge = self.get_temp_edge(&edge_id)?;

                    edges.push(edge);
                }
            }
            _ => {
                for result in iter {
                    let (key, _) = result?;
                    if !key.starts_with(&out_prefix) {
                        break;
                    }

                    let edge_id = String::from_utf8(key[out_prefix.len()..].to_vec())?;
                    let edge = self.get_temp_edge(&edge_id)?;

                    if edge.label.deref() == edge_label {
                        edges.push(edge);
                    }
                }
            }
        }
        Ok(edges)
    }

    fn get_in_edges(&self, node_id: &str, edge_label: &str) -> Result<Vec<Edge>, GraphError> {
        let cf_indices = self
            .db
            .cf_handle(CF_INDICES)
            .ok_or(GraphError::from("Column Family not found"))?;
        let mut edges = Vec::with_capacity(20);
        // get in edges
        let in_prefix = Self::in_edge_key(node_id, "");
        let read_opts = Self::get_optimized_read_options(RAH_MEDIUM);
        let iter = self.db.iterator_cf_opt(
            &cf_indices,
            read_opts,
            IteratorMode::From(&in_prefix, rocksdb::Direction::Forward),
        );

        // get edge values
        match edge_label {
            "" => {
                for result in iter {
                    let (key, _) = result?;
                    if !key.starts_with(&in_prefix) {
                        break;
                    }

                    let edge_id = String::from_utf8(key[in_prefix.len()..].to_vec())?;
                    let edge = self.get_temp_edge(&edge_id)?;

                    edges.push(edge);
                }
            }
            _ => {
                for result in iter {
                    let (key, _) = result?;
                    if !key.starts_with(&in_prefix) {
                        break;
                    }

                    let edge_id = String::from_utf8(key[in_prefix.len()..].to_vec())?;
                    let edge = self.get_temp_edge(&edge_id)?;

                    if edge.label.deref() == edge_label {
                        edges.push(edge);
                    }
                }
            }
        }

        Ok(edges)
    }

    fn get_out_nodes(&self, node_id: &str, edge_label: &str) -> Result<Vec<Node>, GraphError> {
        let cf_indices = self
            .db
            .cf_handle(CF_INDICES)
            .ok_or(GraphError::from("Column Family not found"))?;
        let mut nodes = Vec::with_capacity(20);

        //

        // // Prefetch out edges
        let out_prefix = Self::out_edge_key(node_id, "");
        let read_opts = Self::get_optimized_read_options(RAH_SMALL);
        let iter = self.db.iterator_cf_opt(
            &cf_indices,
            read_opts,
            IteratorMode::From(&out_prefix, rocksdb::Direction::Forward),
        );

        match edge_label {
            "" => {
                for result in iter {
                    let (key, _) = result?;
                    if !key.starts_with(&out_prefix) {
                        break;
                    }
                    let edge =
                        &self.get_temp_edge(&std::str::from_utf8(&key[out_prefix.len()..]).unwrap())?;

                    if let Ok(node) = self.get_temp_node(&edge.to_node) {
                        nodes.push(node);
                    }
                }
            }
            _ => {
                for result in iter {
                    let (key, _) = result?;
                    if !key.starts_with(&out_prefix) {
                        break;
                    }
                    let edge =
                        &self.get_temp_edge(&std::str::from_utf8(&key[out_prefix.len()..]).unwrap())?;

                    if edge.label == edge_label {
                        if let Ok(node) = self.get_temp_node(&edge.to_node) {
                            nodes.push(node);
                        }
                    }
                }
            }
        }

        Ok(nodes)
    }

    fn get_in_nodes(&self, node_id: &str, edge_label: &str) -> Result<Vec<Node>, GraphError> {
        let cf_indices = self
            .db
            .cf_handle(CF_INDICES)
            .ok_or(GraphError::from("Column Family not found"))?;
        let mut nodes = Vec::with_capacity(20);

        // Prefetch in edges
        let in_prefix = Self::in_edge_key(node_id, "");
        let read_opts = Self::get_optimized_read_options(RAH_SMALL);
        let iter = self.db.iterator_cf_opt(
            &cf_indices,
            read_opts,
            IteratorMode::From(&in_prefix, rocksdb::Direction::Forward),
        );

        match edge_label {
            "" => {
                for result in iter {
                    let (key, _) = result?;
                    if !key.starts_with(&in_prefix) {
                        break;
                    }
                    let edge =
                        &self.get_temp_edge(&std::str::from_utf8(&key[in_prefix.len()..]).unwrap())?;

                    if let Ok(node) = self.get_temp_node(&edge.from_node) {
                        nodes.push(node);
                    }
                }
            }
            _ => {
                for result in iter {
                    let (key, _) = result?;
                    if !key.starts_with(&in_prefix) {
                        break;
                    }
                    let edge =
                        &self.get_temp_edge(&std::str::from_utf8(&key[in_prefix.len()..]).unwrap())?;

                    if edge.label == edge_label {
                        if let Ok(node) = self.get_temp_node(&edge.from_node) {
                            nodes.push(node);
                        }
                    }
                }
            }
        }

        Ok(nodes)
    }

    fn get_all_nodes(&self) -> Result<Vec<Node>, GraphError> {
        let cf_nodes = self
            .db
            .cf_handle(CF_NODES)
            .ok_or_else(|| GraphError::from("Column Family not found"))?;

        let approx_size = self
            .db
            .property_int_value_cf(&cf_nodes, "rocksdb.estimate-num-keys")
            .unwrap_or(Some(2000))
            .unwrap_or(2000);
        let mut nodes = Vec::with_capacity(approx_size as usize);
        let node_prefix = Self::node_key("");

        let read_opts = Self::get_optimized_read_options(RAH_LARGE);

        let iter = self.db.iterator_cf_opt(
            &cf_nodes,
            read_opts,
            IteratorMode::From(&node_prefix, rocksdb::Direction::Forward),
        );

        for result in iter.take_while(
            |r| matches!(r, Ok((k, _)) if memchr::memmem::find(k, &node_prefix).is_some()),
        ) {
            let (_, value) = result?;
            if value.is_empty() {
                continue;
            }
            match deserialize::<Node>(&value) {
                Ok(node) => {
                    nodes.push(node);
                }
                Err(e) => {
                    println!("Error Deserializing: {:?}", e);
                    return Err(GraphError::from(format!("Deserialization error: {:?}", e)));
                }
            }
        }
        Ok(nodes)
    }

    fn get_nodes_by_types(&self, types: &[String]) -> Result<Vec<Node>, GraphError> {
        let cf_nodes = self
            .db
            .cf_handle(CF_NODES)
            .ok_or_else(|| GraphError::from("Column Family not found"))?;

        let approx_size = self
            .db
            .property_int_value_cf(&cf_nodes, "rocksdb.estimate-num-keys")
            .unwrap_or(Some(2000))
            .unwrap_or(2000);
        let mut nodes = Vec::with_capacity(approx_size as usize);

        for label in types {
            let node_label_key = [NODE_LABEL_PREFIX, label.as_bytes(), b":"].concat();
            let read_opts = Self::get_optimized_read_options(RAH_SMALL);
            let iter = self.db.iterator_cf_opt(
                &cf_nodes,
                read_opts,
                IteratorMode::From(&node_label_key, rocksdb::Direction::Forward),
            );

            for result in iter.take_while(
                |r| matches!(r, Ok((k, _)) if memchr::memmem::find(k, &node_label_key).is_some()),
            ) {
                let (key, _) = result?;
                let node_id = String::from_utf8(key[node_label_key.len()..].to_vec())?;
                let node = self.get_temp_node(&node_id)?;
                nodes.push(node);
            }
        }

        Ok(nodes)
    }

    fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let cf_edges = self
            .db
            .cf_handle(CF_EDGES)
            .ok_or_else(|| GraphError::from("Column Family not found"))?;

        // Pre-size vector based on metadata if available
        let approx_size = self
            .db
            .property_int_value_cf(&cf_edges, "rocksdb.estimate-num-keys")
            .unwrap_or(Some(2000000))
            .unwrap_or(2000000);

        let mut edges = Vec::with_capacity(approx_size as usize);

        // Optimize read options
        let mut read_opts = ReadOptions::default();
        read_opts.set_readahead_size(RAH_XLARGE);
        read_opts.set_verify_checksums(false); // Skip checksum verification for speed
        read_opts.set_pin_data(true); // Keep data in memory
        read_opts.fill_cache(false); // Don't pollute cache with bulk read

        // Use raw iterator for better performance
        let mut iter = self.db.raw_iterator_cf_opt(&cf_edges, read_opts);
        iter.seek(&Self::edge_key(""));

        // Batch processing
        const BATCH_SIZE: usize = 10000;
        let mut batch = Vec::with_capacity(BATCH_SIZE);

        while iter.valid() {
            let key = iter.key().unwrap();
            if !key.starts_with(&Self::edge_key("")) {
                break;
            }

            if let Some(value) = iter.value() {
                if !value.is_empty() {
                    batch.push(value.to_vec());
                }
            }

            if batch.len() >= BATCH_SIZE {
                // Process batch
                for value in batch {
                    match deserialize::<Edge>(&value) {
                        Ok(edge) => edges.push(edge),
                        Err(e) => {
                            return Err(GraphError::from(format!("Deserialization error: {:?}", e)))
                        }
                    }
                }
                batch = Vec::with_capacity(BATCH_SIZE);
            }

            iter.next();
        }

        // Process remaining batch
        if !batch.is_empty() {
            for value in batch {
                match deserialize::<Edge>(&value) {
                    Ok(edge) => edges.push(edge),
                    Err(e) => {
                        return Err(GraphError::from(format!("Deserialization error: {:?}", e)))
                    }
                }
            }
        }

        Ok(edges)
    }

    fn create_node(
        &self,
        label: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Node, GraphError> {
        let node = Node {
            id: Uuid::new_v4().to_string(),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
        };
        let cf_nodes = self
            .db
            .cf_handle(CF_NODES)
            .ok_or(GraphError::from("Column Family not found"))?;
        let mut new_batch = WriteBatchWithTransaction::default();

        new_batch.put_cf(
            &cf_nodes,
            Self::node_key(&node.id),
            serialize(&node).unwrap(),
        );
        new_batch.put_cf(&cf_nodes, Self::node_label_key(label, &node.id), vec![]);

        self.db.write(new_batch)?;
        Ok(node)
    }

    fn create_edge(
        &self,
        label: &str,
        from_node: &str,
        to_node: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Edge, GraphError> {
        let cf_nodes = self
            .db
            .cf_handle(CF_NODES)
            .ok_or(GraphError::from("Column Family not found"))?;

        if !self
            .db
            .get_pinned_cf(&cf_nodes, Self::node_key(from_node))
            .unwrap()
            .is_some()
            || !self
                .db
                .get_pinned_cf(&cf_nodes, Self::node_key(to_node))
                .unwrap()
                .is_some()
        {
            return Err(GraphError::New(format!("One or both nodes do not exist")));
        } // LOOK INTO BETTER WAY OF DOING THIS

        let edge = Edge {
            id: Uuid::new_v4().to_string(),
            label: label.to_string(),
            from_node: from_node.to_string(),
            to_node: to_node.to_string(),
            properties: HashMap::from_iter(properties),
        };
        let cf_edges = self
            .db
            .cf_handle(CF_EDGES)
            .ok_or(GraphError::from("Column Family not found"))?;

        let cf_indices = self
            .db
            .cf_handle(CF_INDICES)
            .ok_or(GraphError::from("Column Family not found"))?;

        let mut batch = WriteBatch::default();

        // new edge
        batch.put_cf(
            &cf_edges,
            Self::edge_key(&edge.id),
            serialize(&edge).unwrap(),
        );
        // edge label
        batch.put_cf(&cf_indices, Self::edge_label_key(label, &edge.id), vec![]);

        // edge keys
        batch.put_cf(&cf_indices, Self::out_edge_key(from_node, &edge.id), vec![]);
        batch.put_cf(&cf_indices, Self::in_edge_key(to_node, &edge.id), vec![]);

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false);
        write_opts.disable_wal(true);

        self.db.write_opt(batch, &write_opts)?;
        // self.db.write(batch)?;
        Ok(edge)
    }

    fn drop_node(&self, id: &str) -> Result<(), GraphError> {
        let cf_nodes = self
            .db
            .cf_handle(CF_NODES)
            .ok_or(GraphError::from("Column Family not found"))?;

        let read_opts = Self::get_optimized_read_options(RAH_MEDIUM);

        let mut batch = WriteBatch::default();

        // get out edges
        let out_prefix = Self::out_edge_key(id, "");
        let iter = self.db.iterator_cf_opt(
            &cf_nodes,
            read_opts,
            IteratorMode::From(&out_prefix, rocksdb::Direction::Forward),
        );
        // delete them
        for result in iter {
            let (key, _) = result?;
            if !key.starts_with(&out_prefix) {
                break;
            }

            let edge_id = String::from_utf8(key[out_prefix.len()..].to_vec())?;
            let cf_edges = self
                .db
                .cf_handle(CF_EDGES)
                .ok_or(GraphError::from("Column Family not found"))?;
            let edge_data = self
                .db
                .get_pinned_cf(&cf_edges, Self::edge_key(&edge_id))
                .map_err(GraphError::from)?
                .ok_or(GraphError::EdgeNotFound)?;
            let cf_indices = self
                .db
                .cf_handle(CF_INDICES)
                .ok_or(GraphError::from("Column Family not found"))?;

            let edge: Edge = deserialize::<Edge>(&edge_data).unwrap();

            batch.delete_cf(&cf_indices, Self::out_edge_key(&edge.from_node, &edge_id));
            batch.delete_cf(&cf_indices, Self::in_edge_key(&edge.to_node, &edge_id));
            batch.delete_cf(&cf_edges, Self::edge_key(&edge_id));
        }

        let cf_edges = self
            .db
            .cf_handle(CF_EDGES)
            .ok_or(GraphError::from("Column Family not found"))?;
        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(false);
        read_opts.set_readahead_size(2 * 1024 * 1024);
        read_opts.set_prefix_same_as_start(true);
        // get in edges
        let in_prefix = Self::in_edge_key(id, "");
        let iter = self.db.iterator_cf_opt(
            &cf_edges,
            read_opts,
            IteratorMode::From(&in_prefix, rocksdb::Direction::Forward),
        );
        // delete them
        for result in iter {
            let (key, _) = result?;
            if !key.starts_with(&in_prefix) {
                break;
            }

            let edge_id = String::from_utf8(key[out_prefix.len()..].to_vec())?;
            let cf_edges = self
                .db
                .cf_handle(CF_EDGES)
                .ok_or(GraphError::from("Column Family not found"))?;
            let edge_data = self
                .db
                .get_pinned_cf(&cf_edges, Self::edge_key(&edge_id))
                .map_err(GraphError::from)?
                .ok_or(GraphError::EdgeNotFound)?;
            let cf_indices = self
                .db
                .cf_handle(CF_INDICES)
                .ok_or(GraphError::from("Column Family not found"))?;

            let edge = deserialize::<Edge>(&edge_data).unwrap();

            batch.delete_cf(&cf_indices, Self::out_edge_key(&edge.from_node, &edge_id));
            batch.delete_cf(&cf_indices, Self::in_edge_key(&edge.to_node, &edge_id));
            batch.delete_cf(&cf_edges, Self::edge_key(&edge_id));
        }

        // delete node
        batch.delete_cf(&cf_nodes, Self::node_key(id));

        self.db.write(batch).map_err(GraphError::from)
    }

    fn drop_edge(&self, edge_id: &str) -> Result<(), GraphError> {
        let cf_edges = self
            .db
            .cf_handle(CF_EDGES)
            .ok_or(GraphError::from("Column Family not found"))?;
        let edge_data = self
            .db
            .get_pinned_cf(&cf_edges, Self::edge_key(edge_id))
            .map_err(GraphError::from)?
            .ok_or(GraphError::EdgeNotFound)?;
        let cf_indices = self
            .db
            .cf_handle(CF_INDICES)
            .ok_or(GraphError::from("Column Family not found"))?;

        let edge = deserialize::<Edge>(&edge_data).unwrap();

        let mut batch = WriteBatch::default();

        batch.delete_cf(&cf_indices, Self::out_edge_key(&edge.from_node, edge_id));
        batch.delete_cf(&cf_indices, Self::in_edge_key(&edge.to_node, edge_id));
        batch.delete_cf(&cf_edges, Self::edge_key(edge_id));

        match self.db.write(batch) {
            Ok(_) => Ok(()),
            Err(err) => Err(GraphError::from(err)),
        }
    }
}

impl SearchMethods for HelixGraphStorage {
    fn shortest_path(
        &self,
        from_id: &str,
        to_id: &str,
    ) -> Result<(Vec<Node>, Vec<Edge>), GraphError> {
        let cf_indices = self
            .db
            .cf_handle(CF_INDICES)
            .ok_or(GraphError::from("Column Family not found"))?;

        let mut queue = VecDeque::new();
        let mut visited = HashSet::with_capacity(48);
        let mut parent: HashMap<String, (String, Edge)> = HashMap::with_capacity(8);

        queue.push_back(from_id.to_string());
        visited.insert(from_id.to_string());

        let reconstruct_path = move |parent: &HashMap<String, (String, Edge)>,
                                     start_id: &str,
                                     end_id: &str|
              -> Result<(Vec<Node>, Vec<Edge>), GraphError> {
            let mut nodes = Vec::with_capacity(parent.len());
            let mut edges = Vec::with_capacity(parent.len() - 1);
            let mut current = end_id.to_string();

            while current != start_id {
                nodes.push(self.get_temp_node(&current)?);
                let (prev_node, edge) = &parent[current.deref()];
                edges.push(edge.clone());
                current = prev_node.clone();
            }
            nodes.push(self.get_temp_node(start_id)?);

            Ok((nodes, edges))
        };

        while let Some(current_id) = queue.pop_front() {
            let mut read_opts = ReadOptions::default();
            read_opts.set_verify_checksums(false);
            read_opts.set_readahead_size(2 * 1024 * 1024);
            read_opts.set_prefix_same_as_start(true);
            read_opts.set_async_io(true);
            read_opts.set_tailing(true);
            read_opts.fill_cache(false);

            let out_prefix = Self::out_edge_key(&current_id, "");
            let iter = self.db.iterator_cf_opt(
                &cf_indices,
                read_opts,
                IteratorMode::From(&out_prefix, rocksdb::Direction::Forward),
            );
            for result in iter.take_while(
                |r| matches!(r, Ok((k, _)) if memchr::memmem::find(k, &out_prefix).is_some()),
            ) {
                let (key, _) = result?;
                if !key.starts_with(&out_prefix) {
                    break;
                }
                let edge_id = String::from_utf8(key[out_prefix.len()..].to_vec())?;
                let edge = self.get_temp_edge(&edge_id)?;
                let in_v_id = edge.to_node.clone();
                let out_v_id = edge.from_node.clone();
                if !visited.insert(in_v_id.deref().to_string().clone()) {
                    continue;
                }

                parent.insert(
                    in_v_id.deref().to_string().clone(),
                    (out_v_id.deref().to_string(), edge),
                );

                if in_v_id == to_id {
                    return reconstruct_path(&parent, from_id, to_id);
                }

                queue.push_back(in_v_id.deref().to_string());
            }
        }

        Err(GraphError::from(format!(
            "No path found between {} and {}",
            from_id, to_id
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::props;
    use crate::storage_core::storage_methods::StorageMethods;
    use protocol::value::Value;
    use tempfile::TempDir;

    fn setup_temp_db() -> (HelixGraphStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap(); // TODO: Handle Error
        let db_path = temp_dir.path().to_str().unwrap(); // TODO: Handle Error
        let storage = HelixGraphStorage::new(db_path).unwrap(); // TODO: Handle Error
        (storage, temp_dir)
    }

    #[test]
    fn test_create_node() {
        let (storage, _temp_dir) = setup_temp_db();

        let properties = props! {
            "name" => "test node",
        };

        let node = storage.create_node("person", properties).unwrap(); // TODO: Handle Error

        let retrieved_node = storage.get_temp_node(&node.id).unwrap(); // TODO: Handle Error
        assert_eq!(node.id, retrieved_node.id);
        assert_eq!(node.label, "person");
        assert_eq!(
            node.properties.get("name").unwrap(),
            &Value::String("test node".to_string())
        );
    }

    #[test]
    fn test_create_edge() {
        let (storage, _temp_dir) = setup_temp_db();

        let node1 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let node2 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error

        let edge_props = props! {
            "age" => 22,
        };

        let edge = storage
            .create_edge("knows", &node1.id, &node2.id, edge_props)
            .unwrap(); // TODO: Handle Error

        let retrieved_edge = storage.get_temp_edge(&edge.id).unwrap(); // TODO: Handle Error
        assert_eq!(edge.id, retrieved_edge.id);
        assert_eq!(edge.label, "knows");
        assert_eq!(edge.from_node, node1.id);
        assert_eq!(edge.to_node, node2.id);
    }

    #[test]
    fn test_create_edge_with_nonexistent_nodes() {
        let (storage, _temp_dir) = setup_temp_db();

        let result = storage.create_edge("knows", "nonexistent1", "nonexistent2", props!());

        assert!(result.is_err());
    }

    #[test]
    fn test_drop_node() {
        let (storage, _temp_dir) = setup_temp_db();

        let node1 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let node2 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let node3 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error

        storage
            .create_edge("knows", &node1.id, &node2.id, props!())
            .unwrap(); // TODO: Handle Error
        storage
            .create_edge("knows", &node3.id, &node1.id, props!())
            .unwrap(); // TODO: Handle Error

        storage.drop_node(&node1.id).unwrap(); // TODO: Handle Error

        assert!(storage.get_temp_node(&node1.id).is_err());
    }

    #[test]
    fn test_drop_edge() {
        let (storage, _temp_dir) = setup_temp_db();

        let node1 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let node2 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let edge = storage
            .create_edge("knows", &node1.id, &node2.id, props!())
            .unwrap(); // TODO: Handle Error

        storage.drop_edge(&edge.id).unwrap(); // TODO: Handle Error

        assert!(storage.get_temp_edge(&edge.id).is_err());
    }

    #[test]
    fn test_check_exists() {
        let (storage, _temp_dir) = setup_temp_db();

        let node = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        assert!(storage.check_exists(&node.id).unwrap());
        assert!(!storage.check_exists("nonexistent").unwrap());
    }

    #[test]
    fn test_get_temp_node() {
        let (storage, _temp_dir) = setup_temp_db();

        let node = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error

        let temp_node = storage.get_temp_node(&node.id).unwrap(); // TODO: Handle Error

        assert_eq!(node.id, temp_node.id);
        assert_eq!(node.label, temp_node.label);
    }

    #[test]
    fn test_multiple_edges_between_nodes() {
        let (storage, _temp_dir) = setup_temp_db();

        let node1 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let node2 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error

        let edge1 = storage
            .create_edge("knows", &node1.id, &node2.id, props!())
            .unwrap(); // TODO: Handle Error
        let edge2 = storage
            .create_edge("likes", &node1.id, &node2.id, props!())
            .unwrap(); // TODO: Handle Error

        assert!(storage.get_temp_edge(&edge1.id).is_ok());
        assert!(storage.get_temp_edge(&edge2.id).is_ok());
    }

    #[test]
    fn test_node_with_properties() {
        let (storage, _temp_dir) = setup_temp_db();

        let properties = props! {
            "name" => "George",
            "age" => 22,
            "active" => true,
        };
        let node = storage.create_node("person", properties).unwrap(); // TODO: Handle Error
        let retrieved_node = storage.get_temp_node(&node.id).unwrap(); // TODO: Handle Error

        assert_eq!(
            retrieved_node.properties.get("name").unwrap(),
            &Value::String("George".to_string())
        );
        assert!(match retrieved_node.properties.get("age").unwrap() {
            Value::Integer(val) => val == &22,
            Value::Float(val) => val == &22.0,
            _ => false,
        });
        assert_eq!(
            retrieved_node.properties.get("active").unwrap(),
            &Value::Boolean(true)
        );
    }

    #[test]
    fn test_get_all_nodes() {
        let (storage, _temp_dir) = setup_temp_db();
        let node1 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let node2 = storage.create_node("thing", props!()).unwrap(); // TODO: Handle Error
        let node3 = storage.create_node("other", props!()).unwrap(); // TODO: Handle Error

        let nodes = storage.get_all_nodes().unwrap(); // TODO: Handle Error

        assert_eq!(nodes.len(), 3);

        let node_ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();

        assert!(node_ids.contains(&node1.id));
        assert!(node_ids.contains(&node2.id));
        assert!(node_ids.contains(&node3.id));

        let labels: Vec<String> = nodes.iter().map(|n| n.label.clone()).collect();

        assert!(labels.contains(&"person".to_string()));
        assert!(labels.contains(&"thing".to_string()));
        assert!(labels.contains(&"other".to_string()));
    }

    #[test]
    fn test_get_all_node_by_types() {
        let (storage, _temp_dir) = setup_temp_db();
        let node1 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let node2 = storage.create_node("thing", props!()).unwrap(); // TODO: Handle Error
        let node3 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error

        let nodes = storage.get_nodes_by_types(&["person".to_string()]).unwrap(); // TODO: Handle Error

        assert_eq!(nodes.len(), 2);

        let node_ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();

        assert!(node_ids.contains(&node1.id));
        assert!(!node_ids.contains(&node2.id));
        assert!(node_ids.contains(&node3.id));
    }

    #[test]
    fn test_get_all_edges() {
        let (storage, _temp_dir) = setup_temp_db();

        let node1 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let node2 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error
        let node3 = storage.create_node("person", props!()).unwrap(); // TODO: Handle Error

        let edge1 = storage
            .create_edge("knows", &node1.id, &node2.id, props!())
            .unwrap(); // TODO: Handle Error
        let edge2 = storage
            .create_edge("likes", &node2.id, &node3.id, props!())
            .unwrap(); // TODO: Handle Error
        let edge3 = storage
            .create_edge("follows", &node1.id, &node3.id, props!())
            .unwrap(); // TODO: Handle Error

        let edges = storage.get_all_edges().unwrap(); // TODO: Handle Error

        assert_eq!(edges.len(), 3);

        let edge_ids: Vec<String> = edges.iter().map(|e| e.id.clone()).collect();

        assert!(edge_ids.contains(&edge1.id));
        assert!(edge_ids.contains(&edge2.id));
        assert!(edge_ids.contains(&edge3.id));

        let labels: Vec<String> = edges.iter().map(|e| e.label.clone()).collect();

        assert!(labels.contains(&"knows".to_string()));
        assert!(labels.contains(&"likes".to_string()));
        assert!(labels.contains(&"follows".to_string()));

        let connections: Vec<(String, String)> = edges
            .iter()
            .map(|e| (e.from_node.clone(), e.to_node.clone()))
            .collect();

        assert!(connections.contains(&(node1.id.clone(), node2.id.clone())));
        assert!(connections.contains(&(node2.id.clone(), node3.id.clone())));
        assert!(connections.contains(&(node1.id.clone(), node3.id.clone())));
    }

    #[test]
    fn test_shortest_path() {
        let (storage, _temp_dir) = setup_temp_db();
        let mut nodes = Vec::new();
        for _ in 0..6 {
            let node = storage.create_node("person", props!()).unwrap();
            nodes.push(node);
        }

        storage
            .create_edge("knows", &nodes[0].id, &nodes[1].id, props!())
            .unwrap();
        storage
            .create_edge("knows", &nodes[0].id, &nodes[2].id, props!())
            .unwrap();

        storage
            .create_edge("knows", &nodes[1].id, &nodes[3].id, props!())
            .unwrap();
        storage
            .create_edge("knows", &nodes[1].id, &nodes[2].id, props!())
            .unwrap();

        storage
            .create_edge("knows", &nodes[2].id, &nodes[1].id, props!())
            .unwrap();
        storage
            .create_edge("knows", &nodes[2].id, &nodes[3].id, props!())
            .unwrap();
        storage
            .create_edge("knows", &nodes[2].id, &nodes[4].id, props!())
            .unwrap();

        storage
            .create_edge("knows", &nodes[4].id, &nodes[3].id, props!())
            .unwrap();
        storage
            .create_edge("knows", &nodes[4].id, &nodes[5].id, props!())
            .unwrap();

        let shortest_path1 = storage
            .shortest_path(&nodes[0].id, &nodes[5].id)
            .unwrap()
            .1
            .len();
        let shortest_path2 = storage
            .shortest_path(&nodes[1].id, &nodes[5].id)
            .unwrap()
            .1
            .len();
        assert_eq!(shortest_path1, 3);
        assert_eq!(shortest_path2, 3);
    }
}
