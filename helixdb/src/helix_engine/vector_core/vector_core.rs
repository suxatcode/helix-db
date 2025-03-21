use crate::helix_engine::storage_core::storage_core::OUT_EDGES_PREFIX;
use crate::helix_engine::{types::VectorError, vector_core::vector::HVector};
use bincode::{deserialize, serialize};
use heed3::{
    types::{Bytes, Unit},
    Database, Env, RoTxn, RwTxn,
};
use rand::prelude::Rng;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
};

const DB_VECTORS: &str = "vectors"; // for vector data (v:)
const DB_VECTORS_HEADER: &str = "vectors_header"; // for vector header (v:)
const DB_HNSW_OUT_EDGES: &str = "hnsw_out_nodes"; // for hnsw out node data
const VECTOR_PREFIX: &[u8] = b"v:";
const ENTRY_POINT_KEY: &str = "entry_point";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HNSWConfig {
    pub m: usize,            // max num of bi-directional links per element
    pub m_max: usize,        // max num of links for upper layers
    pub ef_construct: usize, // size of the dynamic candidate list for construction
    pub max_elements: usize, // maximum number of elements in the index
    pub m_l: f64,            // level generation factor
    pub max_level: usize,    // max number of levels in index
    pub ef: usize,
}

impl HNSWConfig {
    pub fn new(n: usize) -> Self {
        let m = (2.0 * (n as f64).ln().ceil()) as usize;
        Self {
            m,
            m_max: 2 * m,
            ef_construct: 386,
            max_elements: n,
            m_l: 1.0 / (m as f64).log10(),
            max_level: ((n as f64).log10() / (m as f64).log10()).floor() as usize,
            ef: 800,
        }
    }

    pub fn new_with_params(n: usize, m: usize, ef_construct: usize, ef: usize) -> Self {
        Self {
            m,
            m_max: 2 * m,
            ef_construct,
            max_elements: n,
            m_l: 1.0 / (m as f64).log10(),
            max_level: ((n as f64).log10() / (m as f64).log10()).floor() as usize,
            ef,
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
    vector_header_db: Database<Bytes, Bytes>,
    out_edges_db: Database<Bytes, Unit>,
    pub config: HNSWConfig,
    // TODO: put thread rng here
}

impl VectorCore {
    pub fn new(env: &Env, txn: &mut RwTxn, config: HNSWConfig) -> Result<Self, VectorError> {
        let vectors_db = env.create_database(txn, Some(DB_VECTORS))?;
        let vector_header_db = env.create_database(txn, Some(DB_VECTORS_HEADER))?;
        let out_edges_db = env.create_database(txn, Some(DB_HNSW_OUT_EDGES))?;
        Ok(Self {
            vectors_db,
            vector_header_db,
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
        let r: f64 = rng.random::<f64>();
        let level = (-r.ln() * self.config.m_l).floor() as usize;
        level.min(self.config.max_level)
    }

    #[inline]
    fn get_entry_point(&self, txn: &RoTxn) -> Result<HVector, VectorError> {
        let ep_id = self.vectors_db.get(txn, ENTRY_POINT_KEY.as_bytes())?;
        if let Some(ep_id) = ep_id {
            let ep = self
                .get_vector(
                    txn,
                    unsafe { std::str::from_utf8_unchecked(&ep_id) },
                    0,
                    true,
                )
                .map_err(|_| VectorError::EntryPointNotFound)?;
            Ok(ep)
        } else {
            Err(VectorError::EntryPointNotFound)
        }
    }

    #[inline]
    fn set_entry_point(&self, txn: &mut RwTxn, entry: &HVector) -> Result<(), VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        self.vectors_db
            .put(txn, &entry_key, entry.get_id().as_bytes())
            .map_err(VectorError::from)?;

        Ok(())
    }

    #[inline(always)]
    fn get_vector(
        &self,
        txn: &RoTxn,
        id: &str,
        level: usize,
        with_data: bool,
    ) -> Result<HVector, VectorError> {
        let key = Self::vector_key(id, level);
        match self.vectors_db.get(txn, key.as_ref())? {
            Some(bytes) => {
                let vector = match with_data {
                    true => HVector::from_bytes(id.to_string(), level, &bytes),
                    false => Ok(HVector::from_slice(id.to_string(), level, vec![])),
                }?;
                Ok(vector)
            }
            None if level > 0 => self.get_vector(txn, id, 0, with_data),
            None => Err(VectorError::VectorNotFound(id.to_string())),
        }
    }

    #[inline(always)]
    fn put_vector(&self, txn: &mut RwTxn, vector: &HVector) -> Result<(), VectorError> {
        self.vectors_db
            .put(
                txn,
                &Self::vector_key(vector.get_id(), vector.get_level()),
                vector.to_bytes().as_ref(),
            )
            .map_err(VectorError::from)?;
        Ok(())
    }

    #[inline(always)]
    fn get_neighbors(
        &self,
        txn: &RoTxn,
        id: &str,
        level: usize,
    ) -> Result<Vec<HVector>, VectorError> {
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
                    if let Ok(vector) = self.get_vector(txn, neighbor_id, level, true) {
                        neighbors.push(vector);
                    }
                }
            }
        }
        neighbors.shrink_to_fit();

        Ok(neighbors)
    }

    #[inline(always)]
    fn set_neighbours<'a>(
        &self,
        txn: &mut RwTxn,
        id: &str,
        neighbors: &'a BinaryHeap<HVector>,
        level: usize,
    ) -> Result<(), VectorError> {
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

        Ok(())
    }

    fn select_neighbors<'a>(
        &'a self,
        txn: &RoTxn,
        query: &'a HVector,
        mut cands: BinaryHeap<HVector>,
        level: usize,
        should_extend: bool,
    ) -> Result<BinaryHeap<HVector>, VectorError> {
        let m: usize = if level == 0 {
            self.config.m
        } else {
            self.config.m_max
        };

        if should_extend {
            let mut result = BinaryHeap::with_capacity(m + cands.len());
            for candidate in cands.iter() {
                let neighbors = self.get_neighbors(txn, candidate.get_id(), level)?;
                for mut neighbor in neighbors {
                    neighbor.distance = neighbor.distance_to(query);
                    result.push(neighbor);
                }
            }
            result.extend_inord(cands);
            Ok(result.take_inord(m))
        } else {
            Ok(cands.take_inord(m))
        }
    }

    fn search_level<'a>(
        &'a self,
        txn: &RoTxn,
        query: &'a HVector,
        entry_point: &'a HVector,
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

            for mut neighbor in self.get_neighbors(txn, &curr_cand.id, level)? {
                if visited.contains(neighbor.get_id()) {
                    continue;
                }

                visited.insert(neighbor.get_id().to_string());
                let distance = neighbor.distance_to(query);

                candidates.push(Candidate {
                    id: neighbor.get_id().to_string(),
                    distance,
                });

                if results.len() < ef || distance < results.peek().unwrap().distance {
                    if results.len() > ef {
                        continue;
                    }
                    neighbor.distance = distance;
                    results.push(neighbor);
                }
            }
        }

        Ok(results)
    }

    pub fn search(
        &self,
        txn: &RoTxn,
        query: &[f64],
        k: usize,
    ) -> Result<Vec<HVector>, VectorError> {
        let query = HVector::from_slice("".to_string(), 0, query.to_vec());

        let mut entry_point = self.get_entry_point(txn)?;

        // let ef = (k * 10).max(self.config.ef);
        let ef = self.config.ef;
        let curr_level = entry_point.get_level();

        for level in (1..=curr_level).rev() {
            let mut nearest = self.search_level(txn, &query, &mut entry_point, ef, level)?;
            if let Some(closest) = nearest.pop() {
                entry_point = closest;
            }
        }

        let mut candidates = self.search_level(txn, &query, &mut entry_point, ef, 0)?;

        let mut results = Vec::with_capacity(k);
        for _ in 0..k {
            if let Some(candidate) = candidates.pop() {
                results.push(candidate);
            } else {
                break;
            }
        }

        Ok(results)
    }

    // paper: https://arxiv.org/pdf/1603.09320
    pub fn insert(
        &self,
        txn: &mut RwTxn,
        data: &[f64],
        nid: Option<String>,
    ) -> Result<HVector, VectorError> {
        let id = nid.unwrap_or(uuid::Uuid::new_v4().as_simple().to_string());
        let new_level = self.get_new_level();

        let mut query = HVector::from_slice(id.clone(), 0, data.to_vec());

        self.put_vector(txn, &query)?;
        query.level = new_level;
        self.put_vector(txn, &query)?;

        let entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                self.set_entry_point(txn, &query)?;
                query.distance = 0.0;
                return Ok(query);
            }
        };

        let l = entry_point.get_level();
        let mut curr_ep = entry_point;
        for level in (new_level + 1..=l).rev() {
            let nearest = self.search_level(txn, &query, &curr_ep, 1, level)?;
            curr_ep = nearest.peek().unwrap().clone();
        }

        for level in (0..=l.min(new_level)).rev() {
            let nearest =
                self.search_level(txn, &query, &curr_ep, self.config.ef_construct, level)?;

            curr_ep = nearest.peek().unwrap().clone();

            let neighbors = self.select_neighbors(txn, &query, nearest, level, false)?;

            self.set_neighbours(txn, &query.get_id(), &neighbors, level)?;

            for e in neighbors {
                let id = e.get_id();

                let e_conns = self.get_neighbors(txn, id, level)?;
                // BinaryHeap::from(self.get_neighbors(txn, id, level)?);

                if e_conns.len() > self.config.m_max {
                    let e_conns = BinaryHeap::from(e_conns);
                    let e_new_conn = self.select_neighbors(txn, &query, e_conns, level, true)?;
                    self.set_neighbours(txn, id, &e_new_conn, level)?;
                }
            }
        }

        if new_level > l {
            self.set_entry_point(txn, &query)?;
        }

        Ok(query)
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

    pub fn get_all_vectors_at_level(
        &self,
        txn: &RoTxn,
        level: usize,
    ) -> Result<Vec<HVector>, VectorError> {
        let mut vectors = Vec::new();

        let prefix_iter = self.vectors_db.prefix_iter(txn, VECTOR_PREFIX)?;
        for result in prefix_iter {
            let (_, value) = result?;
            let vector: HVector = deserialize(&value)?;
            if vector.level == level {
                vectors.push(vector);
            }
        }
        Ok(vectors)
    }
}

pub trait Extend<T> {
    /// Extend the heap with another heap
    /// Used because using `.extend()` does not keep the order
    fn extend_inord(&mut self, other: BinaryHeap<T>)
    where
        T: Ord;

    /// Take the top k elements from the heap
    /// Used because using `.iter()` does not keep the order
    fn take_inord(&mut self, k: usize) -> BinaryHeap<T>
    where
        T: Ord;
}

impl<T> Extend<T> for BinaryHeap<T> {
    #[inline(always)]
    fn extend_inord(&mut self, mut other: BinaryHeap<T>)
    where
        T: Ord,
    {
        self.reserve(other.len());
        for candidate in other.drain() {
            self.push(candidate);
        }
    }

    #[inline(always)]
    fn take_inord(&mut self, k: usize) -> BinaryHeap<T>
    where
        T: Ord,
    {
        let mut result = BinaryHeap::with_capacity(k);
        for _ in 0..k {
            if let Some(candidate) = self.pop() {
                result.push(candidate);
            } else {
                break;
            }
        }
        result
    }
}
