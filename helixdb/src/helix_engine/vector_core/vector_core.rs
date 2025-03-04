use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet}, sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
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
const DB_HNSW_IN_EDGES: &str = "hnsw_in_nodes"; // For hnsw in node data

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryPoint {
    id: String,
    level: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DistancedId {
    id: String,
    distance: f64,
}

impl Eq for DistancedId {}

impl PartialOrd for DistancedId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.distance.partial_cmp(&self.distance)
    }
}

impl Ord for DistancedId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

pub struct VectorCore {
    vectors_db: Database<Bytes, Bytes>,
    in_edges_db: Database<Bytes, Unit>,
    out_edges_db: Database<Bytes, Unit>,
    rng_seed: AtomicU64,
    config: HNSWConfig,
}

impl VectorCore {
    pub fn new(
        env: &Env,
        txn: &mut RwTxn,
        config: Option<HNSWConfig>,
    ) -> Result<Self, VectorError> {
        let vectors_db = env.create_database(txn, Some(DB_VECTORS))?;
        let in_edges_db = env.create_database(txn, Some(DB_HNSW_IN_EDGES))?;
        let out_edges_db = env.create_database(txn, Some(DB_HNSW_OUT_EDGES))?;

        let config = config.unwrap_or_default();

        Ok(Self {
            vectors_db,
            in_edges_db,
            out_edges_db,
            rng_seed: AtomicU64::new(0),
            config,
        })
    }

    #[inline]
    fn vector_key(id: &str, level: usize) -> Vec<u8> {
        [
            VECTOR_PREFIX,
            id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
        ]
        .concat()
    }

    #[inline]
    fn in_edges_key(source_id: &str, sink_id: &str, level: usize) -> Vec<u8> {
        [
            IN_EDGES_PREFIX,
            source_id.as_bytes(),
            b":",
            sink_id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
        ]
        .concat()
    }

    #[inline]
    fn out_edges_key(source_id: &str, sink_id: &str, level: usize) -> Vec<u8> {
        [
            OUT_EDGES_PREFIX,
            source_id.as_bytes(),
            b":",
            sink_id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
        ]
        .concat()
    }

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
}

impl HNSW for VectorCore {
    #[inline]
    fn get_random_level(&self) -> usize {
        let mut seed = self.rng_seed.load(AtomicOrdering::Relaxed);
        if seed == 0 { seed = 1; }

        seed ^= seed >> 12;
        seed ^= seed << 25;
        seed ^= seed >> 27;

        self.rng_seed.store(seed, AtomicOrdering::Relaxed);

        let r = ((seed as f64) / (u64::MAX as f64)).abs();

        let level = (-r.ln() * self.config.ml_factor).floor() as usize;
        //println!("Level: {}, r: {}, seed: {}", level, r, seed);
        level
    }

    #[inline]
    fn get_entry_point(&self, txn: &RoTxn) -> Result<EntryPoint, VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();

        let entry_point = match self.vectors_db.get(txn, entry_key.as_ref())? {
            Some(bytes) => deserialize(bytes).map_err(|_| VectorError::InvalidEntryPoint)?,
            None => return Err(VectorError::EntryPointNotFound),
        };
        Ok(entry_point)
    }

    #[inline]
    fn set_entry_point(&self, txn: &mut RwTxn, entry: &EntryPoint) -> Result<(), VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        self.vectors_db
            .put(txn, &entry_key, &serialize(entry)?)
            .map_err(VectorError::from)?;
        Ok(())
    }

    fn get_neighbors(
        &self,
        txn: &RoTxn,
        id: &str,
        level: usize,
    ) -> Result<Vec<String>, VectorError> {
        let out_key = Self::out_edges_key(id, "", level);

        let iter = self
            .out_edges_db
            .lazily_decode_data()
            .prefix_iter(&txn, &out_key)?;

        let mut neighbors = Vec::with_capacity(512);

        for result in iter {
            let (key, _) = result?;
            let neighbor_id = String::from_utf8(key[id.len() + 1..].to_vec())?;
            neighbors.push(neighbor_id);
        }

        Ok(neighbors)
    }

    fn set_neighbors(
        &self,
        txn: &mut RwTxn,
        id: &str,
        level: usize,
        neighbors: &[String],
    ) -> Result<(), VectorError> {
        neighbors
            .iter()
            .try_for_each(|neighbor_id| -> Result<(), VectorError> {
                let out_key = Self::out_edges_key(id, neighbor_id, level);
                let in_key = Self::in_edges_key(neighbor_id, id, level);

                self.out_edges_db.put(txn, &out_key, &())?;
                self.in_edges_db.put(txn, &in_key, &())?;
                Ok(())
            })?;
        Ok(())
    }

    #[inline]
    fn get_vector(&self, txn: &RoTxn, id: &str, level: usize) -> Result<HVector, VectorError> {
        let key = Self::vector_key(id, level);
        match self.vectors_db.get(txn, &key)? {
            Some(bytes) => deserialize(&bytes).map_err(VectorError::from),
            None => {
                if level > 0 {
                    let key = Self::vector_key(id, 0);
                    match self.vectors_db.get(txn, &key)? {
                        Some(bytes) => deserialize(&bytes).map_err(VectorError::from),
                        None => Err(VectorError::VectorNotFound),
                    }
                } else {
                    Err(VectorError::VectorNotFound)
                }
            }
        }
    }

    #[inline]
    fn put_vector(&self, txn: &mut RwTxn, id: &str, vector: &HVector) -> Result<(), VectorError> {
        let key = Self::vector_key(id, vector.level);
        let serialized = serialize(vector).map_err(VectorError::from)?;
        self.vectors_db.put(txn, &key, &serialized)?;
        Ok(())
    }

    fn search_layer(
        &self,
        txn: &RoTxn,
        query: &HVector,
        entry_id: &str,
        ef: usize,
        level: usize,
    ) -> Result<BinaryHeap<DistancedId>, VectorError> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();

        let entry_vector = match self.get_vector(txn, entry_id, level) {
            Ok(v) => v,
            Err(_) if level > 0 => match self.get_vector(txn, entry_id, 0) {
                Ok(v) => v,
                Err(_) => return Ok(BinaryHeap::new()),
            },
            Err(_) => return Ok(BinaryHeap::new()),
        };

        let distance = entry_vector.distance_to(query);

        candidates.push(DistancedId {
            id: entry_id.to_string(),
            distance,
        });

        results.push(DistancedId {
            id: entry_id.to_string(),
            distance,
        });

        visited.insert(entry_id.to_string());

        let expanded_ef = ef.max(10);

        while !candidates.is_empty() {
            let current = candidates.pop().unwrap();

            if results.len() >= expanded_ef {
                if let Some(furthest) = results.peek() {
                    if current.distance > furthest.distance {
                        continue;
                    }
                }
            }

            let neighbors = self.get_neighbors(txn, &current.id, level)?;

            for neighbor_id in neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }

                visited.insert(neighbor_id.clone());

                let neighbor_vector = match self.get_vector(txn, &neighbor_id, level) {
                    Ok(v) => v,
                    Err(_) if level > 0 => match self.get_vector(txn, &neighbor_id, 0) {
                        Ok(v) => v,
                        Err(_) => continue,
                    },
                    Err(_) => continue,
                };

                let distance = neighbor_vector.distance_to(query);

                candidates.push(DistancedId {
                    id: neighbor_id.clone(),
                    distance,
                });

                if results.len() < expanded_ef || distance < results.peek().unwrap().distance {
                    results.push(DistancedId {
                        id: neighbor_id,
                        distance,
                    });

                    if results.len() > expanded_ef {
                        results.pop();
                    }
                }
            }
        }

        Ok(results)
    }

    fn select_neighbors(
        &self,
        txn: &RoTxn,
        candidates: &BinaryHeap<DistancedId>,
        m: usize,
        level: usize,
    ) -> Result<Vec<String>, VectorError> {
        if candidates.len() <= m {
            return Ok(candidates.iter().map(|c| c.id.clone()).collect());
        }

        let mut selected = Vec::with_capacity(m);
        let mut remaining: Vec<_> = candidates.iter().collect();

        remaining.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });

        if !remaining.is_empty() {
            let next = remaining.remove(0);
            selected.push(next.id.clone());
        }

        while selected.len() < m && !remaining.is_empty() {
            let next = remaining.remove(0);
            selected.push(next.id.clone());

            remaining = remaining
                .into_iter()
                .filter(|candidate| {
                    for selected_id in &selected {
                        if &candidate.id == selected_id {
                            continue;
                        }

                        let selected_vector = match self.get_vector(txn, selected_id, level) {
                            Ok(v) => v,
                            Err(_) => return true,
                        };

                        let candidate_vector = match self.get_vector(txn, &candidate.id, level) {
                            Ok(v) => v,
                            Err(_) => return true,
                        };

                        let distance = selected_vector.distance_to(&candidate_vector);

                        if distance < candidate.distance {
                            return false;
                        }
                    }
                    true
                })
                .collect();
        }

        Ok(selected)
    }

    fn search(
        &self,
        txn: &RoTxn,
        query: &HVector,
        k: usize,
    ) -> Result<Vec<(String, f64)>, VectorError> {
        // TODO: make sure input vector is the same dim as all the other vecs
        //let reduced_vec = self.reduce_dims(query.get_data());
        //let query = HVector::from_slice(query.get_id().to_string(), 0, reduced_vec);

        println!("vecs in db: {:?}", self.vectors_db.len(txn)?);

        let entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                let entry_point = EntryPoint {
                    id: "".to_string(),
                    level: 0,
                };
                entry_point
            },
        };

        let query_id = query.get_id();
        let exact_match = self.get_vector(txn, query_id, 0);

        if let Ok(exact_vector) = exact_match {
            if exact_vector.distance_to(&query) < 0.001 {
                return Ok(vec![(query_id.to_string(), 0.0)]);
            }
        }

        let ef = k.max(self.config.ef_construction).max(10);

        let curr_level = entry_point.level;
        let mut curr_id = entry_point.id.clone();

        for level in (1..=curr_level).rev() {
            let nearest = self.search_layer(txn, &query, &curr_id, 1, level)?;
            if !nearest.is_empty() {
                curr_id = nearest.peek().unwrap().id.clone();
            }
        }

        let mut candidates = self.search_layer(txn, &query, &curr_id, ef * 3, 0)?;
        println!("num cands: {}", candidates.len());

        if candidates.is_empty() {
            candidates = self.search_layer(txn, &query, &entry_point.id, ef * 5, 0)?;

            if candidates.is_empty() {
                let all_vectors = self.get_all_vectors(txn)?;
                for vector in all_vectors {
                    if vector.level == 0 {
                        let distance = vector.distance_to(&query);
                        candidates.push(DistancedId {
                            id: vector.get_id().to_string(),
                            distance,
                        });
                    }
                }
            }
        }

        let mut results = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            if let Ok(vector) = self.get_vector(txn, &candidate.id, 0) {
                let exact_distance = vector.distance_to(&query);
                results.push((candidate.id, exact_distance));
            } else {
                results.push((candidate.id, candidate.distance));
            }
        }

        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        if let Some(pos) = results.iter().position(|(id, _)| id == query_id) {
            if pos > 0 {
                let item = results.remove(pos);
                results.insert(0, item);
            }
        }

        println!("results len: {}", results.len());

        if results.len() > k {
            results.truncate(k);
        }

        Ok(results)
    }

    fn insert(&self, txn: &mut RwTxn, id: &str, data: &[f64]) -> Result<(), VectorError> {
        let random_level = self.get_random_level();

        // TODO: make sure input vector is the same dim as all the other vecs
        //let reduced_vec = self.reduce_dims(data);
        //let vector = HVector::from_slice(id.to_string(), 0, reduced_vec.clone());
        let vector = HVector::from_slice(id.to_string(), 0, data.to_vec());

        self.put_vector(txn, id, &vector)?;

        if random_level > 0 {
            //let higher_vector = HVector::from_slice(id.to_string(), random_level, reduced_vec);
            let higher_vector = HVector::from_slice(id.to_string(), random_level, data.to_vec());
            self.put_vector(txn, id, &higher_vector)?;
        }

        let entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                let entry_point = EntryPoint {
                    id: id.to_string(),
                    level: random_level,
                };
                self.set_entry_point(txn, &entry_point)?;
                entry_point
            },
        };

        let current_ep = entry_point;

        let curr_id = current_ep.id.clone();
        let mut curr_level = current_ep.level;

        if random_level > curr_level {
            let new_ep = EntryPoint {
                id: id.to_string(),
                level: random_level,
            };
            self.set_entry_point(txn, &new_ep)?;
            curr_level = random_level;
        }

        let mut ep_id = curr_id;

        if ep_id != id {
            let neighbors = vec![ep_id.clone()];
            self.set_neighbors(txn, id, 0, &neighbors)?;

            let mut ep_neighbors = self.get_neighbors(txn, &ep_id, 0)?;
            ep_neighbors.push(id.to_string());
            self.set_neighbors(txn, &ep_id, 0, &ep_neighbors)?;
        }

        for level in (1..=random_level).rev() {
            if level <= curr_level {
                let ef = self.config.ef_construction * 2;
                let nearest = self.search_layer(txn, &vector, &ep_id, ef, level)?;

                if nearest.is_empty() {
                    continue;
                }

                let m = if level == 0 {
                    self.config.m
                } else {
                    self.config.m_max
                };

                let neighbors = self.select_neighbors(txn, &nearest, m, level)?;

                self.set_neighbors(txn, id, level, &neighbors)?;

                for neighbor_id in &neighbors {
                    let mut neighbor_neighbors = self.get_neighbors(txn, neighbor_id, level)?;
                    neighbor_neighbors.push(id.to_string());

                    if neighbor_neighbors.len() > m {
                        let neighbor_vector = match self.get_vector(txn, neighbor_id, level) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        let candidates: BinaryHeap<_> = neighbor_neighbors
                            .iter()
                            .filter_map(|n_id| match self.get_vector(txn, n_id, level) {
                                Ok(n_vector) => {
                                    let dist = neighbor_vector.distance_to(&n_vector);
                                    Some(DistancedId {
                                        id: n_id.clone(),
                                        distance: dist,
                                    })
                                }
                                Err(_) => None,
                            })
                            .collect();

                        let pruned = self.select_neighbors(txn, &candidates, m, level)?;
                        self.set_neighbors(txn, neighbor_id, level, &pruned)?;
                    } else {
                        self.set_neighbors(txn, neighbor_id, level, &neighbor_neighbors)?;
                    }
                }

                if !nearest.is_empty() {
                    ep_id = nearest.peek().unwrap().id.clone();
                }
            }
        }

        Ok(())
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
}
