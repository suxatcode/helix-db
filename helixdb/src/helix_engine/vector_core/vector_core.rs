use super::hnsw::HNSW;
use crate::helix_engine::vector_core::vector::HVector;
use crate::helix_engine::{storage_core::storage_core::OUT_EDGES_PREFIX, types::VectorError};
use bincode::{deserialize, serialize};
use heed3::{
    types::{Bytes, Unit},
    Database, Env, RoTxn, RwTxn,
};
use indexmap::IndexMap;
use rand::prelude::Rng;
use rand::seq::IndexedRandom;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

const DB_VECTORS: &str = "vectors"; // for vector data (v:)
const DB_HNSW_OUT_EDGES: &str = "hnsw_out_nodes"; // for hnsw out node data

const VECTOR_PREFIX: &[u8] = b"v:";
const ENTRY_POINT_KEY: &str = "entry_point";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HNSWConfig {
    pub m: usize,                 // maximum number of connections per element
    pub m_max: usize,             // maximum number of connections for upper layers
    pub ef_construction: usize,   // size of the dynamic candidate list for construction
    pub max_elements: usize,      // maximum number of elements in the index
    pub m_l: f64,                 // level generation factor
    pub distance_multiplier: f64, // distance multiplier for pruning
    pub max_level: usize,         // max number of levels in index
    pub target_dimension: Option<usize>,
}

pub struct VectorCore {
    vectors_db: Database<Bytes, Bytes>,
    out_edges_db: Database<Bytes, Unit>,
    rng_seed: AtomicU64,
    config: HNSWConfig,
    // TODO: optim configs here, not in hnswconfig
}

#[derive(PartialEq)]
struct Candidate {
    id: String,
    distance: f64,
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.distance.partial_cmp(&self.distance)
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl Default for HNSWConfig {
    fn default() -> Self {
        Self {
            m: 20,
            m_max: 32,
            ef_construction: 200,
            max_elements: 1_000_000,
            m_l: 0.36,
            distance_multiplier: 1.5,
            target_dimension: None,
            max_level: 5,
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
        // TODO: assert dim is correct and same as all others
        // TODO: implement this on HVector? like HVector::
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
    pub fn get_new_level(&self) -> usize {
        // // Atomically update the RNG seed
        // let mut seed = self.rng_seed.fetch_add(1, AtomicOrdering::Relaxed);
        // if seed == 0 {
        //     seed = 1;
        // }
        
        // // XorShift64* algorithm
        // seed ^= seed >> 12;
        // seed ^= seed << 25;
        // seed ^= seed >> 27;
        // let random_value = seed.wrapping_mul(0x2545F4914F6CDD1D);
        
        // // Convert to [0,1) range
        // let r = (random_value as f64) / (u64::MAX as f64);
        
        // // Level calculation with exponential distribution
        // // Use a constant like 1/ln(M) where M is base parameter (often 2-16)
        // // Alternatively, you can use a fixed value like self.config.m_l
        // let level = (-r.ln() * self.config.m_l).floor() as usize;
        
        // // Cap the maximum level to prevent extremely rare but very high levels
        // println!("level: {:?}, max_level: {:?}", level, self.config.max_level);
        // level.min(self.config.max_level)

        let mut rng = rand::rng();
        let level = (-rng.random::<f64>().ln()).floor() as usize;
        level.min(self.config.max_level)
    }

    #[inline]
    fn get_entry_point(&self, txn: &RoTxn) -> Result<HVector, VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        let entry_point_bytes = match self.vectors_db.get(txn, entry_key.as_ref())? {
            Some(bytes) => bytes,
            None => return Err(VectorError::EntryPointNotFound),
        };

        let vector: HVector = deserialize(entry_point_bytes)
            .map_err(|_| VectorError::InvalidEntryPoint)?;

        Ok(vector)
    }

    #[inline]
    fn set_entry_point(&self, txn: &mut RwTxn, entry: &HVector) -> Result<(), VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        self.vectors_db
            .put(txn, &entry_key, &serialize(entry)?)
            .map_err(VectorError::from)?;

        Ok(())
    }

    #[inline(always)]
    fn get_vector(&self, txn: &RoTxn, id: &str, level: usize) -> Result<HVector, VectorError> {
        let key = Self::vector_key(id, level);
        match self.vectors_db.get(txn, key.as_ref())? {
            Some(bytes) => deserialize(&bytes).map_err(VectorError::from),
            None if level > 0 => self.get_vector(txn, id, 0),
            None => Err(VectorError::VectorNotFound(id.to_string())),
        }
    }

    #[inline(always)]
    fn put_vector(&self, txn: &mut RwTxn, vector: &HVector) -> Result<(), VectorError> {
        self.vectors_db
            .put(
                txn,
                &Self::vector_key(vector.get_id(), vector.get_level()),
                &serialize(vector)?,
            )
            .map_err(VectorError::from)
    }

    #[inline(always)]
    fn get_neighbors(&self, txn: &RoTxn, id: &str, level: usize) -> Result<Vec<HVector>, VectorError> {
        let out_key = Self::out_edges_key(id, "", level);

        let iter = self
            .out_edges_db
            .lazily_decode_data()
            .prefix_iter(&txn, &out_key)?;

        let mut neighbors = Vec::with_capacity(512);
        let prefix_len = OUT_EDGES_PREFIX.len() + id.len() + 1 + level.to_string().len() + 1;

        for result in iter {
            // key = source_id:sink_id
            let (key, _) = result?;
            let neighbor_id = std::str::from_utf8(&key[prefix_len..])?;
            if neighbor_id == id {
                continue;
            }
            neighbors.push(self.get_vector(txn, neighbor_id, level)?); // TODO: can this be better
        }

        Ok(neighbors)
    }

    #[inline(always)]
    fn set_neighbours(&self, txn: &mut RwTxn, id: &str, neighbors: &BinaryHeap<HVector>, level: usize) -> Result<(), VectorError> {
        neighbors
            .iter()
            .try_for_each(|neighbor| -> Result<(), VectorError> {
                let neighbor_id = neighbor.get_id();
                if neighbor_id == id {
                    return Ok(());
                }
                let out_key = Self::out_edges_key(id, neighbor_id, level);
                let in_key = Self::out_edges_key(neighbor_id, id, level);

                self.out_edges_db.put(txn, &out_key, &())?;
                self.out_edges_db.put(txn, &in_key, &())?;
                Ok(())
            })?;
        Ok(())
    }

    fn select_neighbors(
        &self,
        txn: &RoTxn,
        candidates: &BinaryHeap<HVector>,
        level: usize,
        extend_cands: bool,
        _keep_prund_cands: bool, // we remove this option for clarity
    ) -> Result<BinaryHeap<HVector>, VectorError> {
        let m = if level == 0 {
            self.config.m
        } else {
            self.config.m_max
        };
    
        // Start by including the candidates themselves.
        let mut all_candidates = IndexMap::new();
        for candidate in candidates {
            all_candidates.insert(candidate.get_id().to_string(), candidate.clone());
            if extend_cands {
                // Get neighbors from the graph for each candidate.
                for neighbor in self.get_neighbors(txn, candidate.get_id(), level)? {
                    all_candidates.entry(neighbor.get_id().to_string()).or_insert(neighbor);
                }
            }
        }
    
        // Convert to a Vec and sort by distance (ascending order).
        let mut sorted_candidates: Vec<_> = all_candidates.into_iter().map(|(_id, vec)| vec).collect();
        sorted_candidates.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal));
    
        // Select the best m candidates.
        let selected = sorted_candidates.into_iter().take(m);
        let mut neighbor_heap = BinaryHeap::with_capacity(m);
        for candidate in selected {
            neighbor_heap.push(candidate);
        }
    
        Ok(neighbor_heap)
    }

    fn search_level(
        &self,
        txn: &RoTxn,
        query: &HVector,
        entry_point: &HVector,
        ef: usize,
        level: usize,
    ) -> Result<BinaryHeap<HVector>, VectorError> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut candidates: BinaryHeap<Candidate> = BinaryHeap::new();
        let mut results: BinaryHeap<HVector> = BinaryHeap::new();

        candidates.push(Candidate {
            id: entry_point.get_id().to_string(),
            distance: entry_point.distance,
        });
        results.push(entry_point.clone());
        visited.insert(entry_point.get_id().to_string());

        while !candidates.is_empty() {
            let curr_cand = candidates.pop().unwrap();

            if results.len() >= ef
                && results
                    .peek()
                    .map_or(false, |f| curr_cand.distance > f.distance)
            {
                break;
            }

            let neighbors = self.get_neighbors(txn, &curr_cand.id, level)?;

            for mut neighbor in neighbors {
                if !visited.contains(neighbor.get_id()) {
                    visited.insert(neighbor.get_id().to_string());

                    let distance = neighbor.distance_to(query);

                    candidates.push(Candidate {
                        id: neighbor.get_id().to_string(),
                        distance,
                    });
                    if results.len() < ef || distance < results.peek().unwrap().distance {
                        neighbor.distance = distance;
                        results.push(neighbor.clone());

                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn search(&self, txn: &RoTxn, query: &HVector, k: usize) -> Result<Vec<HVector>, VectorError> {
        // TODO: do a check first before going through reduce dims to avoid clone if not needed
        //let reduced_vec = self.reduce_dims(query.get_data()); // TODO: general optim traits thing
        //let query = HVector::from_slice("".to_string(), 0, reduced_vec.clone());

        let mut entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                return Err(VectorError::EntryPointNotFound);
            }
        };

        let ef = k.max(self.config.ef_construction);
        let curr_level = entry_point.get_level();

        for level in (1..=curr_level).rev() {
            let nearest = self.search_level(txn, &query, &mut entry_point, ef, level)?;
            if !nearest.is_empty() {
                std::mem::replace(&mut entry_point, nearest.peek().unwrap().clone()); // TODO: do better (no clone)
            }
        }

        let candidates = self.search_level(txn, &query, &mut entry_point, ef * 5, 0)?; // TODO: if we get nothing, add a change in precision mechanism for ef

        let mut results: Vec<HVector> = Vec::with_capacity(candidates.len());
        candidates.iter().for_each(|c| {
            let mut n_c = c.clone();
            n_c.distance = n_c.distance_to(&query);
            results.push(n_c);
        });

        results.truncate(k);
        Ok(results)
    }

    // paper: https://arxiv.org/pdf/1603.09320
    pub fn insert(&self, txn: &mut RwTxn, data: &[f64]) -> Result<HVector, VectorError> {
        let id = uuid::Uuid::new_v4().as_simple().to_string();
        //let reduced_vec = self.reduce_dims(data);
        //let data_query = HVector::from_slice(id.clone(), 0, reduced_vec.clone()); // TODO: Optimise this
        let new_level = self.get_new_level();

        let mut data_query = HVector::from_slice(id.clone(), 0, data.to_vec());
        self.put_vector(txn, &data_query)?;
        data_query.level = new_level;
        self.put_vector(txn, &data_query)?;


        let entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                let mut entry_point = HVector::from_slice(id.to_string(), new_level, data.to_vec());
                entry_point.distance = 0.0;
                self.set_entry_point(txn, &entry_point)?;
                entry_point
            }
        };

        let l = entry_point.get_level();
        let mut curr_ep = entry_point;
        for level in (new_level + 1..=l).rev() {
            let nearest = self.search_level(txn, &data_query, &curr_ep, 1, level)?;
            curr_ep = nearest.peek().unwrap().clone();
        }

        for level in (0..=l.min(new_level)).rev() {
            let nearest = self.search_level(txn, &data_query, &curr_ep, self.config.ef_construction, level)?;
            let neighbors = self.select_neighbors(txn, &nearest, level, true, true)?;

            
            self.set_neighbours(txn, &data_query.get_id(), &neighbors, level)?; // TODO possibly remove?
            // TODO: add bi-directional connections from neighbors to q at level
            for e in neighbors {
                let e_conn = BinaryHeap::from(self.get_neighbors(txn, e.get_id(), level)?);
                if e_conn.len() > self.config.m_max {
                    let e_new_conn = self.select_neighbors(txn, &e_conn, level, true, true)?;
                    self.set_neighbours(txn, e.get_id(), &e_new_conn, level)?;
                }
            }
        }

        if new_level > l {
            self.set_entry_point(txn, &data_query)?;
        }

        

        Ok(data_query)
    }

    fn get_all_vectors(&self, txn: &RoTxn) -> Result<Vec<HVector>, VectorError> {
        let mut vectors = Vec::new();

        let prefix_iter = self.vectors_db.prefix_iter(txn, VECTOR_PREFIX)?;
        for result in prefix_iter {
            let (_, value) = result?;
            let vector: HVector = deserialize(&value)?;
            vectors.push(vector);
        }
        Ok(vectors)
    }


    /* TODO
    fn delete(&self, txn: &mut RwTxn, ...) -> Result<String, VectorError> {
        // self.search or something
    }
    */
}
