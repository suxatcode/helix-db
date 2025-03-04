use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

use bincode::{deserialize, serialize};
use heed3::{
    types::{Bytes, Unit},
    Database, Env, RoTxn, RwTxn,
};
use serde::{Deserialize, Serialize};

use crate::helix_engine::vector_core::vector::HVector;
use crate::helix_engine::{
    storage_core::storage_core::{IN_EDGES_PREFIX, OUT_EDGES_PREFIX},
    types::VectorError,
};

use super::hnsw::HNSW;

const DB_VECTORS: &str = "vectors"; // For vector data (v:)
const DB_HNSW_OUT_EDGES: &str = "hnsw_out_nodes"; // For hnsw out node data

const VECTOR_PREFIX: &[u8] = b"v:";
const ENTRY_POINT_KEY: &str = "entry_point";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HNSWConfig {
    pub m: usize,                 // Maximum number of connections per element
    pub m_max: usize,             // Maximum number of connections for upper layers
    pub ef_construction: usize,   // Size of the dynamic candidate list for construction
    pub max_elements: usize,      // Maximum number of elements in the index
    pub ml_factor: f64,           // Level generation factor
    pub distance_multiplier: f64, // Distance multiplier for pruning
    pub target_dimension: Option<usize>,
}

impl Default for HNSWConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max: 32,
            ef_construction: 200,
            max_elements: 1_000_000,
            ml_factor: 1.0 / std::f64::consts::LN_2,
            distance_multiplier: 1.5,
            target_dimension: None,
        }
    }
}

impl HNSWConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn calc_target_dim(original_dim: usize) -> usize {
        let sqrt_dim = (original_dim as f64).sqrt().ceil() as usize;
        let log_dim = ((original_dim as f64).log2() * 2.0).ceil() as usize;
        let percent_dim = (original_dim as f64 * 0.2).ceil() as usize;

        let mut dims = vec![sqrt_dim, log_dim, percent_dim];
        dims.sort_unstable();
        let target_dim = dims[1];

        target_dim.clamp(3, original_dim.min(256))
    }

    pub fn with_dim_reduce(original_dim: usize, n: Option<usize>) -> Self {
        Self {
            target_dimension: Some(match n {
                Some(dim) => dim,
                None => Self::calc_target_dim(original_dim),
            }),
            ..Self::default()
        }
    }
}

pub struct VectorCore {
    vectors_db: Database<Bytes, Bytes>,
    out_edges_db: Database<Bytes, Unit>,
    rng_seed: AtomicU64,
    // entry_point: Box<Option<HVector>>,
    config: HNSWConfig,
    // TODO: optim configs here, not in hnswconfig
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryPoint {
    id: String,
    level: usize,
}

impl VectorCore {
    pub fn new(
        env: &Env,
        txn: &mut RwTxn,
        config: Option<HNSWConfig>,
    ) -> Result<Self, VectorError> {
        let vectors_db = env.create_database(txn, Some(DB_VECTORS))?;
        let out_edges_db = env.create_database(txn, Some(DB_HNSW_OUT_EDGES))?;

        let config = config.unwrap_or_default();

        Ok(Self {
            vectors_db,
            out_edges_db,
            rng_seed: AtomicU64::new(0),
            // entry_point: Box::new(None),
            config,
        })
    }

    #[inline(always)]
    fn vector_key(id: &str, level: usize) -> Vec<u8> {
        [
            VECTOR_PREFIX,
            id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
        ]
        .concat()
    }

    #[inline(always)]
    fn out_edges_key(source_id: &str, sink_id: &str, level: usize) -> Vec<u8> {
        [
            OUT_EDGES_PREFIX,
            source_id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
            b":",
            sink_id.as_bytes(),
        ]
        .concat()
    }

    /// (pooling operation reduce)
    fn reduce_dims(&self, data: &[f64]) -> Vec<f64> {
        let target_dim = match self.config.target_dimension {
            None => return data.to_vec(),
            Some(dim) => dim,
        };

        if data.len() <= target_dim {
            return data.to_vec();
        }

        let chunk_size = (data.len() as f64 / target_dim as f64).ceil() as usize;
        let mut reduced = Vec::with_capacity(target_dim);

        for chunk_idx in 0..target_dim {
            let start = chunk_idx * chunk_size;
            let end = (start + chunk_size).min(data.len());

            if start >= data.len() {
                break;
            }

            let avg = data[start..end].iter().sum::<f64>() / (end - start) as f64;
            reduced.push(avg);
        }

        reduced
    }

    #[inline]
    fn get_random_level(&self) -> usize {
        let mut seed = self.rng_seed.load(AtomicOrdering::Relaxed);
        if seed == 0 {
            seed = 1;
        }

        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;

        self.rng_seed.store(seed, AtomicOrdering::Relaxed);
        let r = ((seed as f64) / (u64::MAX as f64)).abs();
        let level = (-r.ln() * self.config.ml_factor).floor() as usize;

        level
    }

 
    #[inline]
    fn get_entry_point(&self, txn: &RoTxn) -> Result<HVector, VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();

        let entry_point: EntryPoint = match self.vectors_db.get(txn, entry_key.as_ref())? {
            Some(bytes) => deserialize(bytes).map_err(|_| VectorError::InvalidEntryPoint)?,
            None => return Err(VectorError::EntryPointNotFound),
        };
        let vector_key = Self::vector_key(entry_point.id.as_str(), entry_point.level);
        let vector: HVector = match self.vectors_db.get(txn, vector_key.as_ref())? {
            Some(bytes) => deserialize(bytes).map_err(|_| VectorError::InvalidEntryPoint)?,
            None => return Err(VectorError::EntryPointNotFound),
        };
        Ok(vector)
    }

    #[inline]
    fn set_entry_point(&self, txn: &mut RwTxn, entry: &HVector) -> Result<(), VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        let vector_key = Self::vector_key(entry.get_id(), entry.get_level());
        self.vectors_db.put(txn, &vector_key, &serialize(entry)?)?;

        let entry_point = EntryPoint {
            id: entry.get_id().to_string(),
            level: entry.get_level(),
        };
        self.vectors_db
            .put(txn, &entry_key, &serialize(&entry_point)?)
            .map_err(VectorError::from)?;

        Ok(())
    }

    fn search(&self, txn: &RoTxn, query: &HVector, k: usize) -> Result<Vec<(String, f64)>, VectorError> {
        let reduced_vec = self.reduce_dims(data); // TODO: general optim traits thing
        let vector = HVector::from_slice(id.to_string(), 0, reduced_vec.clone());

        let random_level = self.get_random_level();
        let mut cache = HashMap::with_capacity(500);

        let mut entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                return Err(VectorError::EntryPointNotFound);
            }
        };

        let query_id = query.get_id();
        let ef = k.max(self.config.ef_construction).max(10); // TODO: Remove hardcoded 10
        let curr_level = entry_point.get_level();

        for level in (1..=curr_level).rev() {
            let nearest = self.search_layer(txn, &query, &entry_point, 10, level, cache)?;
            assert_eq!(nearest.len(), 1, "Search layer should return 1 result");
            if !nearest.empty() {
                entry_point = self.get_vector(txn, &nearest.peek().unwrap().id, 0)?;
            }
        }

        let mut candidates = self.search_layer(txn, &query, &entry_point, ef * 5, 0)?; // TODO: if we get nothing, add a change in precision mechanism

        let results = Vec::with_capacity(candidates.len());
        candidates.iter().for_each(|c| {
            results.push((c.get_id().clone(), c.distance_to(&query)));
        });
        Ok(results)
    }

    /**
     * #[derive(Debug, Clone, PartialEq)]
        pub struct DistancedId {
            id: String,
            distance: f64,
        }
     */
    fn search_layer(
        &self,
        txn: &RoTxn,
        query: &HVector,
        entry_point: &HVector,
        ef: usize,
        level: usize,
        cands: &mut HashMap<String, HVector>,
    ) -> Result<BinaryHeap<HVector>, VectorError> {

    }

    // fn search_and_return_string(&self, txn: &RoTxn, query: &HVector, k: usize) -> Result<Vec<String>, VectorError> {
    //     let hashmap = HashMap::with_capacity(500);

        

    //     for (string, _ ) in self.search(txn, query, k)? {
    //         // get string data from lmdb 
    //     }

    // }

    fn insert(&self, txn: &mut RwTxn, data: &[f64]) -> Result<String, VectorError> {
    }

    
}