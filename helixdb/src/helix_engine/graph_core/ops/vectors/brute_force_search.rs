use heed3::RoTxn;

use super::super::tr_val::TraversalVal;
use crate::helix_engine::{
    graph_core::traversal_iter::RoTraversalIterator,
    types::{GraphError, VectorError},
    vector_core::{
        hnsw::HNSW,
        vector::{cosine_similarity, HVector},
    },
};
use std::{collections::BinaryHeap, iter::once};
use crate::helix_storage::Storage;

pub struct BruteForceSearchV<I: Iterator<Item = Result<TraversalVal, GraphError>>> {
    iter: I,
}

// implementing iterator for OutIterator
impl<I: Iterator<Item = Result<TraversalVal, GraphError>>> Iterator for BruteForceSearchV<I> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait BruteForceSearchVAdapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn brute_force_search_v(
        self,
        query: &Vec<f64>,
        k: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, S> BruteForceSearchVAdapter<'a, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a,
    S: Storage + ?Sized,
{
    fn brute_force_search_v(
        self,
        query: &Vec<f64>,
        k: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let mut iter_vec: Vec<_> = self.inner.collect();

        iter_vec.sort_by(|v1, v2| {
            if let (Ok(TraversalVal::Vector(v1)), Ok(TraversalVal::Vector(v2))) = (v1, v2) {
                let d1 = cosine_similarity(v1.get_data(), query).unwrap_or(0.0);
                let d2 = cosine_similarity(v2.get_data(), query).unwrap_or(0.0);
                d2.partial_cmp(&d1).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                std::cmp::Ordering::Equal
            }
        });

        let iter = iter_vec.into_iter().take(k);

        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
