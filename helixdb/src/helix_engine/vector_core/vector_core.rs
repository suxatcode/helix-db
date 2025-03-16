use crate::helix_engine::vector_core::vector::HVector;
use crate::helix_engine::{storage_core::storage_core::OUT_EDGES_PREFIX, types::VectorError};
use bincode::{deserialize, serialize};
use heed3::{
    types::{Bytes, Unit},
    Database, Env, RoTxn, RwTxn,
};
use indexmap::IndexMap;
use rand::prelude::Rng;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
};

const DB_VECTORS: &str = "vectors"; // for vector data (v:)
const DB_HNSW_OUT_EDGES: &str = "hnsw_out_nodes"; // for hnsw out node data

const VECTOR_PREFIX: &[u8] = b"v:";
const ENTRY_POINT_KEY: &str = "entry_point";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HNSWConfig {
    pub m: usize,            // max num of bi-directional links per element
    pub m_max: usize,        // max num of links for upper layers
    pub ef_construct: usize, // size of the dynamic candidate list for construction
    pub ef_c: usize,         // ef_search factor (usually 10 so that 10*k)
    pub max_elements: usize, // maximum number of elements in the index
    pub m_l: f64,            // level generation factor
    pub max_level: usize,    // max number of levels in index
}

impl HNSWConfig {
    pub fn new(n: usize) -> Self {
        let d = (10.0 + 20.0 * (10_000.0_f64.log10() / (n as f64).log10())).floor() as usize;
        let o_m = 5.max(48.min(d));
        //let o_m = (2.0 * (n as f64).ln().ceil()) as usize;
        Self {
            m: o_m,
            m_max: 2 * o_m,
            ef_construct: 400,
            ef_c: 10,
            max_elements: n,
            m_l: 1.0 / (o_m as f64).log10(),
            max_level: ((n as f64).log10() / (o_m as f64).log10()).floor() as usize,
        }
    }
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

pub struct VectorCore {
    vectors_db: Database<Bytes, Bytes>,
    out_edges_db: Database<Bytes, Unit>,
    pub config: HNSWConfig,
}

impl VectorCore {
    pub fn new(env: &Env, txn: &mut RwTxn, n: usize) -> Result<Self, VectorError> {
        let vectors_db = env.create_database(txn, Some(DB_VECTORS))?;
        let out_edges_db = env.create_database(txn, Some(DB_HNSW_OUT_EDGES))?;
        let config = HNSWConfig::new(n);
        Ok(Self {
            vectors_db,
            out_edges_db,
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

    #[inline]
    pub fn get_new_level(&self) -> usize {
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

        let vector: HVector =
            deserialize(entry_point_bytes).map_err(|_| VectorError::InvalidEntryPoint)?;

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
    fn get_neighbors(
        &self,
        txn: &RoTxn,
        id: &str,
        level: usize,
    ) -> Result<Vec<HVector>, VectorError> {
        let start_time = Instant::now();
        let out_key = Self::out_edges_key(id, "", level);
        let mut neighbors = Vec::with_capacity(self.config.m_max.min(512));

        let iter = self
            .out_edges_db
            .lazily_decode_data()
            .prefix_iter(txn, &out_key)?;

        let prefix_len = out_key.len();
        let id_bytes = id.as_bytes();

        for result in iter {
            if let Ok((key, _)) = result {
                let neighbor_id = unsafe { std::str::from_utf8_unchecked(&key[prefix_len..]) };

                if neighbor_id.as_bytes() != id_bytes {
                    if let Ok(vector) = self.get_vector(txn, neighbor_id, level) {
                        neighbors.push(vector);
                    }
                }
            }
        }
        let time = start_time.elapsed();
        // println!("get_neighbors: {} ms", time.as_millis());
        neighbors.shrink_to_fit();
        Ok(neighbors)
    }

    /*
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
    */

    #[inline(always)]
    fn set_neighbours(
        &self,
        txn: &mut RwTxn,
        id: &str,
        neighbors: &BinaryHeap<HVector>,
        level: usize,
    ) -> Result<(), VectorError> {
        let start_time = Instant::now();
        let prefix = Self::out_edges_key(id, "", level);

        let mut keys_to_delete: HashSet<Vec<u8>> = self
            .out_edges_db
            .prefix_iter(txn, prefix.as_ref())?
            .filter_map(|result| result.ok().map(|(key, _)| key.to_vec()))
            .collect();

        neighbors
            .iter()
            .try_for_each(|neighbor| -> Result<(), VectorError> {
                let neighbor_id = neighbor.get_id();
                if neighbor_id == id {
                    return Ok(());
                }
                let out_key = Self::out_edges_key(id, neighbor_id, level);
                keys_to_delete.remove(&out_key);
                self.out_edges_db.put(txn, &out_key, &())?;

                let in_key = Self::out_edges_key(neighbor_id, id, level);
                keys_to_delete.remove(&in_key);
                self.out_edges_db.put(txn, &in_key, &())?;

                Ok(())
            })?;

        for key in keys_to_delete {
            self.out_edges_db.delete(txn, &key)?;
        }
        let time = start_time.elapsed();
        // println!("set_neighbors: {} ms", time.as_millis());
        Ok(())
    }

    fn select_neighbors(
        &self,
        cands: &BinaryHeap<HVector>,
        level: usize,
    ) -> Result<BinaryHeap<HVector>, VectorError> {
        let start_time = Instant::now();
        let m = if level == 0 {
            self.config.m
        } else {
            self.config.m_max
        };

        let mut candidates: Vec<_> = cands.into_iter().cloned().collect();

        candidates.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });

        let selected = candidates.into_iter().take(m);

        let mut neighbor_heap = BinaryHeap::with_capacity(m);
        for candidate in selected {
            neighbor_heap.push(candidate);
        }

        let time = start_time.elapsed();
        // println!("select_neighbors: {} ms", time.as_millis());

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
        let start_time = Instant::now();
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
            let neighbour_start_time = Instant::now();
            let neighbors = self.get_neighbors(txn, &curr_cand.id, level)?;
            let neighbour_time = neighbour_start_time.elapsed();
            // println!(
            //     "search_level:\n\tget_neighbors: {} ms",
            //     neighbour_time.as_millis()
            // );

            let loop_start_time = Instant::now();
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

        let time = start_time.elapsed();
        // println!("search_level: {} ms", time.as_millis());

        Ok(results)
    }

    pub fn search(
        &self,
        txn: &RoTxn,
        query: &[f64],
        k: usize,
    ) -> Result<Vec<HVector>, VectorError> {
        let query = HVector::from_slice("".to_string(), 0, query.to_vec());

        let mut entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                return Err(VectorError::EntryPointNotFound);
            }
        };

        let ef_search = self.config.ef_c * k;
        let curr_level = entry_point.get_level();

        for level in (1..=curr_level).rev() {
            let nearest = self.search_level(txn, &query, &mut entry_point, ef_search, level)?;
            if !nearest.is_empty() {
                std::mem::replace(&mut entry_point, nearest.peek().unwrap().clone());
                // TODO: do better (no clone)
            }
        }

        let candidates = self.search_level(txn, &query, &mut entry_point, ef_search, 0)?; // TODO: if we get nothing, add a change in precision mechanism for ef

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
    pub fn insert(&self, txn: &mut RwTxn, data: &[f64]) -> Result<(), VectorError> {
        let id = uuid::Uuid::new_v4().as_simple().to_string();
        let new_level = self.get_new_level();

        let mut query = HVector::from_slice(id.clone(), 0, data.to_vec());

        self.put_vector(txn, &query)?;
        query.level = new_level;
        self.put_vector(txn, &query)?;

        let entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                query.distance = 0.0;
                self.set_entry_point(txn, &query)?;
                return Ok(());
            }
        };

        let l = entry_point.get_level();
        let mut curr_ep = entry_point;
        for level in (new_level + 1..=l).rev() {
            let nearest = self.search_level(txn, &query, &curr_ep, 1, level)?;
            curr_ep = nearest.peek().unwrap().clone();
        }

        for level in (0..=l.min(new_level)).rev() {
            let start_time = Instant::now();
            let nearest =
                self.search_level(txn, &query, &curr_ep, self.config.ef_construct, level)?;
            let time = start_time.elapsed();

            let start_time = Instant::now();
            let neighbors = self.select_neighbors(&nearest, level)?;
            let time = start_time.elapsed();

            self.set_neighbours(txn, &query.get_id(), &neighbors, level)?;

            for e in neighbors {
                let id = e.get_id();

                let start_time = Instant::now();
                let e_conn = BinaryHeap::from(self.get_neighbors(txn, id, level)?);
                let time = start_time.elapsed();

                if e_conn.len() > self.config.m_max {
                    let e_new_conn = self.select_neighbors(&e_conn, level)?;
                    self.set_neighbours(txn, id, &e_new_conn, level)?;
                } else {
                    self.set_neighbours(txn, id, &e_conn, level)?;
                }
            }
        }

        if new_level > l {
            self.set_entry_point(txn, &query)?;
        }
        // println!();
        Ok(())
    }

    pub fn get_all_vectors(&self, txn: &RoTxn) -> Result<Vec<HVector>, VectorError> {
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
