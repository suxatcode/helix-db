use std::collections::HashMap;

use crate::{helix_engine::types::VectorError, protocol::value::Value};
use crate::helix_engine::vector_core::vector::HVector;
use heed3::{RoTxn, RwTxn};

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
    fn search<F>(
        &self,
        txn: &RoTxn,
        query: &[f64],
        k: usize,
        filter: Option<&[F]>,
        should_trickle: bool,
    ) -> Result<Vec<HVector>, VectorError>
    where
        F: Fn(&HVector) -> bool;

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
    /// An HVector of the data inserted
    fn insert<F>(
        &self,
        txn: &mut RwTxn,
        data: &[f64],
        nid: Option<u128>,
        fields: Option<HashMap<String, Value>>,
    ) -> Result<HVector, VectorError>
    where
        F: Fn(&HVector) -> bool;

    /// Load a full hnsw index with all vectors at once
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `id` - The id of the vector
    /// * `data` - A Vec of all the vectors to insert
    ///
    /// # Returns
    ///
    /// An emtpy tuple
    fn load<F>(&self, txn: &mut RwTxn, data: Vec<&[f64]>) -> Result<(), VectorError>
    where
        F: Fn(&HVector) -> bool;

    /// Get all vectors from the index
    ///
    /// # Arguments
    ///
    /// * `txn` - The read-only transaction to use for retrieving vectors
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec` of `HVector` if successful
    fn get_all_vectors(&self, txn: &RoTxn) -> Result<Vec<HVector>, VectorError>;

    /// Get all vectors from the index at a specific level
    ///
    /// # Arguments
    ///
    /// * `txn` - The read-only transaction to use for retrieving vectors
    /// * `level` - A usize for which level to get all vectors from
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec` of `HVector` if successful
    fn get_all_vectors_at_level(
        &self,
        txn: &RoTxn,
        level: usize,
    ) -> Result<Vec<HVector>, VectorError>;

    // Get the number of vectors in the hnsw index
    //
    // # Returns
    //
    // A `usize` of the number of vecs
    //fn get_num_of_vecs(&self) -> usize;
}
