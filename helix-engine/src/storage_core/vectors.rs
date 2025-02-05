// vector struct to store raw data, dimension and de

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    path::Path,
};

use bincode::deserialize;
use heed3::{types::Bytes, Database, EnvOpenOptions, RoTxn, RwTxn};
use rand::Rng;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::GraphError;

use super::storage_methods::{BasicStorageMethods, StorageMethods, VectorMethods};

const DB_VECTORS: &str = "vectors"; // For vector data (v:)
const DB_HNSW_OUT_NODES: &str = "hnsw_out_nodes"; // For hnsw out node data
const DB_HNSW_IN_NODES: &str = "hnsw_in_nodes"; // For hnsw in node data

const VECTOR_PREFIX: &[u8] = b"v:";
const OUT_PREFIX: &[u8] = b"o:";
const IN_PREFIX: &[u8] = b"i:";

#[repr(C)]
#[derive(Copy, Clone)]
pub struct HVector<'v> {
    data: &'v [f64],
}

pub trait EuclidianDistance {
    fn distance<'a>(from: &'a HVector, to: &'a HVector) -> f64;
}
impl<'v> EuclidianDistance for HVector<'v> {
    fn distance<'a>(from: &'a HVector, to: &'a HVector) -> f64 {
        from.data
            .iter()
            .zip(to.data.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f64>()
            .sqrt()
    }
}

#[repr(C)]
#[derive(Clone)]
pub struct HVectorNode {
    id: Uuid,
    level: usize,
    max_layer: usize,
}

impl HVectorNode {
    pub fn new(id: Uuid, level: usize, max_layer: usize) -> Self {
        HVectorNode {
            id,
            level,
            max_layer,
        }
    }
}

impl<'v> HVector<'v> {
    pub fn new(data: &'v [f64]) -> Self {
        HVector { data }
    }

    pub fn get_data(&self) -> &'v [f64] {
        self.data
    }

    /// Returns the vector as a slice of bytes
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            // converts the slice of f64 to a slice of u8
            std::slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                self.data.len() * std::mem::size_of::<f64>(),
            )
        }
    }

    /// Returns the vector as a slice of bytes
    pub unsafe fn from_bytes(bytes: &'v [u8]) -> Result<HVector<'v>, GraphError> {
        if bytes.len() != std::mem::size_of::<Self>() {
            return Err(GraphError::Default);
        }
        // converts the slice of u8 to a slice of f64
        Ok(*(bytes.as_ptr() as *const HVector))
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[repr(C)]
#[derive(Serialize, Deserialize)]
pub struct HNSWConfig<
    const EF_C: usize,
    const EF: usize,
    const M: usize,
    const M_0: usize,
    const V_D: usize,
> {
    ef_construction: usize, // how many nearest neighbors to visit duing layer construction
    ef: usize,              // how many nearest neighbors to visit in a search
    m: usize, // number of nearest-neighbors to connect a new entry to when it is inserted (should be 5-48)
    m_0: usize, // max m for 0th layer (2 * m)
    m_l: usize, // controls random selection (1 / ln(m))
    m_max: usize, // Max m for each layer (will set as m)
    vd: usize, // Dimension of the vector
}

pub struct HNSWMetadata {
    entry_point: Option<Uuid>,
    max_layer: usize,
    node_count: usize,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub struct HNeighbour<'a> {
    id: &'a str,
    distance: f64,
}
impl<'a> Ord for HNeighbour<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
            .reverse()
    }
}
impl<'a> Eq for HNeighbour<'a> {}

pub struct HNSW<
    const EF_C: usize,
    const EF: usize,
    const M: usize,
    const M_0: usize,
    const V_D: usize,
> {
    pub metadata: HNSWMetadata,
    pub config: HNSWConfig<EF_C, EF, M, M_0, V_D>,
    pub vectors_db: Database<Bytes, Bytes>,
    pub hnsw_out_nodes_db: Database<Bytes, Bytes>,
    pub hnsw_in_nodes_db: Database<Bytes, Bytes>,
}

impl<const EF_C: usize, const EF: usize, const M: usize, const M_0: usize, const V_D: usize>
    HNSW<EF_C, EF, M, M_0, V_D>
{
    pub fn new(path: &str) -> Result<HNSW<EF_C, EF, M, M_0, V_D>, GraphError> {
        let vector_env = unsafe {
            EnvOpenOptions::new()
                .map_size(20 * 1024 * 1024 * 1024) // 10GB max
                .max_dbs(12)
                .max_readers(200)
                .open(Path::new(path))?
        };
        let mut wtxn = vector_env.write_txn()?;

        let vectors_db = vector_env.create_database(&mut wtxn, Some(DB_VECTORS))?;
        let hnsw_out_nodes_db = vector_env.create_database(&mut wtxn, Some(DB_HNSW_OUT_NODES))?;
        let hnsw_in_nodes_db = vector_env.create_database(&mut wtxn, Some(DB_HNSW_IN_NODES))?;
        wtxn.commit()?;

        Ok(HNSW {
            metadata: HNSWMetadata {
                entry_point: None,
                max_layer: 0,
                node_count: 0,
            },
            config: HNSWConfig::<EF_C, EF, M, M_0, V_D> {
                ef_construction: 200,
                ef: 50,
                m: M,
                m_0: M_0,
                m_l: 1 / (M as f64).ln() as usize,
                m_max: M,
                vd: V_D,
            },
            vectors_db,
            hnsw_out_nodes_db,
            hnsw_in_nodes_db,
        })
    }

    #[inline(always)]
    pub fn vector_key(id: &str) -> Vec<u8> {
        [VECTOR_PREFIX, id.as_bytes()].concat()
    }

    #[inline(always)]
    pub fn out_prefix(id: &str) -> Vec<u8> {
        [OUT_PREFIX, id.as_bytes()].concat()
    }

    pub fn generate_random_level(&self) -> usize {
        let mut rng = rand::rng();
        let mut level = 0;
        while rng.random::<f64>() < 1.0 / (self.config.m_l as f64).ln() {
            // ln
            level += 1;
        }
        level
    }

    #[inline(always)]
    pub fn store_vec(
        &self,
        txn: &mut RwTxn,
        vec_id: &str,
        vector: HVector,
    ) -> Result<(), GraphError> {
        Ok(self
            .vectors_db
            .put(txn, vec_id.as_bytes(), vector.as_bytes())?)
    }

    /// Best fit algorithm to find neighbours
    pub fn search_layer<'a>(
        &'a self,
        txn: &'a RoTxn,
        layer: usize,
        query: HVector,
        ef: usize,
    ) -> Result<Vec<HNeighbour<'a>>, GraphError> {
        let start = self.metadata.entry_point;
        let mut visited: HashSet<&str> = HashSet::with_capacity(100);
        let mut candidates: BinaryHeap<HNeighbour> = BinaryHeap::with_capacity(100);
        let mut nn: BinaryHeap<HNeighbour> = BinaryHeap::with_capacity(100); // nearest neighbours
        let start_id = start.unwrap().to_string();
        visited.insert(start_id.as_str());
        candidates.push(HNeighbour {
            id: start_id.as_str(),
            distance: HVector::distance(&self.get_vector(txn, start_id.as_str())?, &query),
        });

        // c <- get nearest element from candidate to query
        while let Some(current) = candidates.pop() {
            // f <- get furthest element from nearest neighbours to query
            let furthest_dist: f64 = nn.peek().map_or(f64::INFINITY, |n| n.distance);

            // if distance(c,query) > distance(f, query) break
            if current.distance > furthest_dist {
                break;
            }

            for e in self.connections(txn, &current.id)? {
                if visited.insert(e) {
                    let dist = HVector::distance(&self.get_vector(txn, e)?, &query);

                    let neighbour = HNeighbour {
                        id: e,
                        distance: dist,
                    };
                    if dist < furthest_dist {
                        candidates.push(neighbour.clone());
                        nn.push(neighbour);
                        if nn.len() > ef {
                            nn.pop();
                        }
                    }
                }
            }
        }

        Ok(nn.into_sorted_vec())
    }

    pub fn select_neighbours<'a>(
        &'a self,
        txn: &'a RoTxn,
        _query: HVector,
        candidates: &[HNeighbour<'a>],
        level: usize,
    ) -> Result<Vec<&'a str>, GraphError> {
        let max_conns = if level == 0 { M_0 } else { M };
        let mut result = Vec::with_capacity(max_conns);

        for candidate in candidates.iter().take(max_conns) {
            let mut should_add = true;
            for &existing in &result {
                let dist_between = HVector::distance(
                    &self.get_vector(txn, candidate.id)?,
                    &self.get_vector(txn, existing)?,
                );

                if dist_between < candidate.distance {
                    should_add = false;
                    break;
                }
            }

            if should_add {
                result.push(candidate.id);
            }
        }

        Ok(result)
    }
}

impl<
        'a,
        const EF_C: usize,
        const EF: usize,
        const M: usize,
        const M_0: usize,
        const V_D: usize,
    > VectorMethods<'a> for HNSW<EF_C, EF, M, M_0, V_D>
{
    fn get_vector(&'a self, txn: &'a RoTxn<'a>, id: &str) -> Result<HVector<'a>, GraphError> {
        match self.vectors_db.get(txn, id.as_bytes())? {
            Some(data) => unsafe { HVector::from_bytes(data) },
            None => Err(GraphError::New(format!("Vector not found: {}", id))),
        }
    }

    fn insert(&mut self, txn: &mut RwTxn, id: &str, data: &[f64]) -> Result<(), GraphError> {
        // for vector, get entry point via log dist
        let vec_id = Uuid::new_v4();
        let level = self.generate_random_level();
        let mut new_node = HVectorNode::new(vec_id, level, level);

        // If the index is empty, initialize with the new node.
        if self.metadata.entry_point.is_none() {
            self.metadata.entry_point = Some(vec_id);
            self.metadata.max_layer = level;

            return Ok(());
        }

        // Start the search from the global entry point.
        let mut current = self.metadata.entry_point.unwrap();
        let mut current_dist = HVector::distance(
            &self.get_vector(txn, current.to_string().as_str())?,
            &HVector::new(data),
        );
        // store vector

        // insert from top level down
        // for each level find best neighbours at that level
        // created edges
        // update entry points with node id

        for level in (level..=self.metadata.max_layer) {
            let neighbours = self.search_layer(txn, level, HVector::new(data),  1)?;
            self.metadata.entry_point =
                Some(Uuid::parse_str(neighbours.first().unwrap().id).unwrap());
        }

        for level in (level..=self.metadata.max_layer).rev() {
            let neighbours = self.search_layer(txn, level, HVector::new(data),if level == 0 { EF_C } else { M })?;
            let selected = self.select_neighbours(txn, HVector::new(data), &neighbours, level)?;


        }

        self.vectors_db
            .put(txn, id.as_bytes(), HVector::new(data).as_bytes())?;

        Ok(())
    }

    fn connections(&'a self, txn: &'a RoTxn<'a>, id: &str) -> Result<Vec<&str>, GraphError> {
        Ok(self
            .hnsw_out_nodes_db
            .lazily_decode_data()
            .prefix_iter(txn, &Self::out_prefix(id))?
            .filter_map(|res| match res {
                Ok((key, _)) => {
                    Some(std::str::from_utf8(&key[id.len()..]).map_err(GraphError::from))
                }
                Err(_) => None,
            })
            .collect::<Result<Vec<&str>, GraphError>>()?)
    }
}

impl<const EF_C: usize, const EF: usize, const M: usize, const M_0: usize, const V_D: usize>
    BasicStorageMethods for HNSW<EF_C, EF, M, M_0, V_D>
{
    fn get_temp_edge<'a>(&self, txn: &'a RoTxn, id: &str) -> Result<&'a [u8], GraphError> {
        Err(GraphError::New("Not implemented".to_string()))
    }
    fn get_temp_node<'a>(&self, txn: &'a RoTxn, id: &str) -> Result<&'a [u8], GraphError> {
        match self.vectors_db.get(txn, id.as_bytes())? {
            Some(data) => Ok(data),
            None => Err(GraphError::New(format!("Node not found: {}", id))),
        }
    }
}
