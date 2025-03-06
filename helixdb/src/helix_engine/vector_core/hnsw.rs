use heed3::{RoTxn, RwTxn};
use crate::helix_engine::types::VectorError;
use crate::helix_engine::vector_core::vector::HVector;

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
    fn search(&self, txn: &RoTxn, query: &HVector, k: usize) -> Result<Vec<HVector>, VectorError>;

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
    fn insert(&self, txn: &mut RwTxn, data: &[f64]) -> Result<String, VectorError>;

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
}
