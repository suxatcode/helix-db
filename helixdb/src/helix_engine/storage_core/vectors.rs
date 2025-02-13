// vector struct to store raw data, dimension and de

use std::{
    cmp::Ordering, collections::{BinaryHeap, HashSet}, path::Path, sync::{Arc, Mutex}, vec
};

use bincode::deserialize;
use heed3::{types::Bytes, Database, Env, EnvOpenOptions, RoTxn, RwTxn};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::num::ParseIntError;

use crate::helix_engine::types::GraphError;

use super::storage_methods::{BasicStorageMethods, StorageMethods, VectorMethods};

const DB_VECTORS: &str = "vectors"; // For vector data (v:)
const DB_HNSW_OUT_NODES: &str = "hnsw_out_nodes"; // For hnsw out node data
const DB_HNSW_IN_NODES: &str = "hnsw_in_nodes"; // For hnsw in node data

const VECTOR_PREFIX: &[u8] = b"v:";
const OUT_PREFIX: &[u8] = b"o:";
const IN_PREFIX: &[u8] = b"i:";

#[repr(C, align(16))]  // Align to 16 bytes for better SIMD performance
#[derive(Clone)]
pub struct HVector {
    data: Vec<f64>,
}

pub trait EuclidianDistance {
    fn distance(from: &HVector, to: &HVector) -> f64;
}

impl EuclidianDistance for HVector {
    #[inline(always)]
    fn distance(from: &HVector, to: &HVector) -> f64 {
        // Fast path: use SIMD for aligned data of same length
        if from.len() == to.len() {
            unsafe {
                return from.simd_distance_unchecked(to);
            }
        }
        
        // Fallback to scalar implementation for different lengths
        from.scalar_distance(to)
    }
}

impl HVector {
    #[inline(always)]
    pub fn new(data: Vec<f64>) -> Self {
        HVector { data }
    }

    #[inline(always)]
    pub fn from_slice(data: &[f64]) -> Self {
        HVector { data: data.to_vec() }
    }

    #[inline(always)]
    pub fn get_data(&self) -> &[f64] {
        &self.data
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let size = self.data.len() * std::mem::size_of::<f64>();
        let mut bytes = Vec::with_capacity(size);
        for &value in &self.data {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, GraphError> {
        if bytes.len() % std::mem::size_of::<f64>() != 0 {
            return Err(GraphError::Default);
        }

        let mut data = Vec::with_capacity(bytes.len() / std::mem::size_of::<f64>());
        let chunks = bytes.chunks_exact(std::mem::size_of::<f64>());
        
        for chunk in chunks {
            let value = f64::from_le_bytes(chunk.try_into().unwrap());
            data.push(value);
        }

        Ok(HVector { data })
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline(always)]
    pub fn distance_to(&self, other: &HVector) -> f64 {
        HVector::distance(self, other)
    }

    // Internal methods for distance calculation
    #[inline(always)]
    unsafe fn simd_distance_unchecked(&self, other: &HVector) -> f64 {
        use std::arch::aarch64::{vld1q_f64, vsubq_f64, vmulq_f64, vaddvq_f64};
        
        let mut sum = 0.0;
        let n = self.len();
        let mut i = 0;

        // Process 2 elements at a time using NEON SIMD
        while i + 2 <= n {
            let a = vld1q_f64(self.data[i..].as_ptr());
            let b = vld1q_f64(other.data[i..].as_ptr());
            let diff = vsubq_f64(a, b);
            let squared = vmulq_f64(diff, diff);
            sum += vaddvq_f64(squared);
            i += 2;
        }

        // Handle remaining elements
        while i < n {
            let diff = self.data[i] - other.data[i];
            sum += diff * diff;
            i += 1;
        }

        sum.sqrt()
    }

    #[inline(always)]
    fn scalar_distance(&self, other: &HVector) -> f64 {
        let mut sum = 0.0;
        let n = self.len().min(other.len());
        
        // Use iterator for better bounds check elimination
        self.data[..n]
            .iter()
            .zip(other.data[..n].iter())
            .for_each(|(x, y)| {
                let diff = x - y;
                sum += diff * diff;
            });

        sum.sqrt()
    }
}

#[cfg(test)]
mod vector_tests {
    use super::*;

    #[test]
    fn test_hvector_new() {
        let data = vec![1.0, 2.0, 3.0];
        let vector = HVector::new(data);
        assert_eq!(vector.get_data(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_hvector_from_slice() {
        let data = [1.0, 2.0, 3.0];
        let vector = HVector::from_slice(&data);
        assert_eq!(vector.get_data(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn test_hvector_distance() {
        let v1 = HVector::new(vec![1.0, 0.0]);
        let v2 = HVector::new(vec![0.0, 1.0]);
        let distance = HVector::distance(&v1, &v2);
        assert!((distance - 2.0_f64.sqrt()).abs() < 1e-10);
    }

    #[test]
    fn test_hvector_distance_zero() {
        let v1 = HVector::new(vec![1.0, 2.0, 3.0]);
        let v2 = HVector::new(vec![1.0, 2.0, 3.0]);
        let distance = HVector::distance(&v1, &v2);
        assert!(distance.abs() < 1e-10);
    }

    #[test]
    fn test_hvector_distance_to() {
        let v1 = HVector::new(vec![0.0, 0.0]);
        let v2 = HVector::new(vec![3.0, 4.0]);
        let distance = v1.distance_to(&v2);
        assert!((distance - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_bytes_roundtrip() {
        let original = HVector::new(vec![1.0, 2.0, 3.0]);
        let bytes = original.to_bytes();
        let reconstructed = HVector::from_bytes(&bytes).unwrap();
        assert_eq!(original.get_data(), reconstructed.get_data());
    }

    #[test]
    fn test_hvector_len() {
        let data = vec![1.0, 2.0, 3.0, 4.0];
        let vector = HVector::new(data);
        assert_eq!(vector.len(), 4);
    }

    #[test]
    fn test_hvector_is_empty() {
        let empty_vector = HVector::new(vec![]);
        let non_empty_vector = HVector::new(vec![1.0, 2.0]);
        
        assert!(empty_vector.is_empty());
        assert!(!non_empty_vector.is_empty());
    }

    #[test]
    fn test_hvector_distance_different_dimensions() {
        let v1 = HVector::new(vec![1.0, 2.0, 3.0]);
        let v2 = HVector::new(vec![1.0, 2.0, 3.0, 4.0]);
        let distance = HVector::distance(&v1, &v2);
        assert!(distance.is_finite());
    }

    #[test]
    fn test_hvector_large_values() {
        let v1 = HVector::new(vec![1e6, 2e6]);
        let v2 = HVector::new(vec![1e6, 2e6]);
        let distance = HVector::distance(&v1, &v2);
        assert!(distance.abs() < 1e-10);
    }

    #[test]
    fn test_hvector_negative_values() {
        let v1 = HVector::new(vec![-1.0, -2.0]);
        let v2 = HVector::new(vec![1.0, 2.0]);
        let distance = HVector::distance(&v1, &v2);
        assert!((distance - (20.0_f64).sqrt()).abs() < 1e-10);
    }
}
