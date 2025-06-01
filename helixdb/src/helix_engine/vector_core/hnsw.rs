use std::collections::HashMap;

use crate::{helix_engine::types::VectorError, protocol::value::Value};
use crate::helix_engine::vector_core::vector::HVector;
use heed3::{RoTxn, RwTxn};

use super::vector::encoding;

pub trait HNSW<E: encoding::Encoding, const DIMENSION: usize>
{
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
    fn search<F, const WITH_VECTOR: bool, const WITH_PROPERTIES: bool>(
        &self,
        txn: &RoTxn,
        query: [E; DIMENSION],
        k: usize,
        filter: Option<&[F]>,
        should_trickle: bool,
    ) -> Result<Vec<HVector<E, DIMENSION>>, VectorError>
    where
        F: Fn(&HVector<E, DIMENSION>, &RoTxn) -> bool;

    /// Insert a new vector into the index
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `data` - The vector data
    ///
    /// # Returns
    ///
    /// An HVector of the data inserted
    fn insert<F>(
        &self,
        txn: &mut RwTxn,
        data: [E; DIMENSION],
        fields: Option<Vec<(String, Value)>>,
    ) -> Result<HVector<E, DIMENSION>, VectorError>
    where
        F: Fn(&HVector<E, DIMENSION>, &RoTxn) -> bool;

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
    fn get_all_vectors(
        &self,
        txn: &RoTxn,
        level: Option<u8>,
    ) -> Result<Vec<HVector<E, DIMENSION>>, VectorError>;

    /// Get specific vector based on id and level
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `id` - The id of the vector
    /// * `level` - Which level to get the vector from
    /// * `with_data` - Whether or not to fetch the vector with data
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec` of `HVector` if successful
    fn get_vector<const WITH_VECTOR: bool, const WITH_PROPERTIES: bool>(
        &self,
        txn: &RoTxn,
        id: u128,
        level: u8,
    ) -> Result<HVector<E, DIMENSION>, VectorError>;
}

