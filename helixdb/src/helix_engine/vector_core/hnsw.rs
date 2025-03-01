use std::collections::BinaryHeap;

use heed3::{RoTxn, RwTxn};

use crate::helix_engine::types::VectorError;
use crate::helix_engine::vector_core::vector::HVector;

use super::vector_core::{DistancedId, EntryPoint};

pub trait HNSW {
    /// Search for the k nearest neighbors of a query vector
    /// 
    /// # Arguments
    /// 
    /// * `txn` - The transaction to use
    /// * `query` - The query vector
    /// * `k` - The number of nearest neighbors to search for
    /// 
    /// # Returns
    /// 
    /// A vector of tuples containing the id and distance of the nearest neighbors
    fn search(
        &self,
        txn: &RoTxn,
        query: &HVector,
        k: usize,
    ) -> Result<Vec<(String, f64)>, VectorError>;

    /// Insert a new vector into the index
    /// 
    /// # Arguments
    /// 
    /// * `txn` - The transaction to use
    /// * `id` - The id of the vector
    /// * `data` - The vector data
    /// 
    /// # Returns
    /// 
    /// An empty tuple
    fn insert(&mut self, txn: &mut RwTxn, id: &str, data: &[f64]) -> Result<(), VectorError>;

    /// Get all vectors from the index
    fn get_all_vectors(&self, txn: &RoTxn) -> Result<Vec<HVector>, VectorError>;

    /// Get the entry point of the index
    fn get_entry_point(&self) -> Result<&Option<EntryPoint>, VectorError>;

    /// Set the entry point of the index
    fn set_entry_point(&mut self, txn: &mut RwTxn, entry: &EntryPoint) -> Result<(), VectorError>;

    /// Get a random level
    fn get_random_level(&mut self) -> usize;

    /// Get a vector from the index
    fn get_vector(&self, txn: &RoTxn, id: &str, level: usize) -> Result<HVector, VectorError>;

    /// Put a vector into the index
    fn put_vector(&self, txn: &mut RwTxn, id: &str, vector: &HVector) -> Result<(), VectorError>;

    /// Get the neighbors of a vector
    /// 
    /// # Arguments
    /// 
    /// * `txn` - The transaction to use
    /// * `id` - The id of the vector
    /// * `level` - The level of the vector
    /// 
    /// # Returns
    /// 
    /// A vector of ids of the neighbors
    fn get_neighbors(
        &self,
        txn: &RoTxn,
        id: &str,
        level: usize,
    ) -> Result<Vec<String>, VectorError>;

    /// Set the neighbors of a vector
    fn set_neighbors(
        &self,
        txn: &mut RwTxn,
        id: &str,
        level: usize,
        neighbors: &[String],
    ) -> Result<(), VectorError>;

    /// Select the neighbors of a vector
    fn select_neighbors(
        &self,
        txn: &RoTxn,
        query: &HVector,
        candidates: &BinaryHeap<DistancedId>,
        m: usize,
        level: usize,
    ) -> Result<Vec<String>, VectorError>;

    /// Search a layer of the index
    fn search_layer(
        &self,
        txn: &RoTxn,
        query: &HVector,
        entry_id: &str,
        ef: usize,
        level: usize,
    ) -> Result<BinaryHeap<DistancedId>, VectorError>;
}
