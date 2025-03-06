use heed3::{Env, EnvOpenOptions};
use std::collections::HashSet;
use rand::{rngs::StdRng, Rng, SeedableRng, prelude::SliceRandom};
use polars::prelude::*;

use super::vector::HVector;
use crate::helix_engine::vector_core::vector_core::VectorCore;

fn setup_temp_env() -> Env {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    unsafe {
        EnvOpenOptions::new()
            .map_size(512 * 1024 * 1024) // 10MB
            .max_dbs(10)

            .open(path)
            .unwrap()
    }
}

fn generate_random_vectors(count: usize, dim: usize, seed: u64) -> Vec<(String, Vec<f64>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut vectors = Vec::with_capacity(count);

    for i in 0..count {
        let id = format!("vec_{}", i);
        let data: Vec<f64> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        vectors.push((id, data));
    }

    vectors
}

fn euclidean_distance(v1: &[f64], v2: &[f64]) -> f64 {
    v1.iter().zip(v2.iter()).map(|(&a, &b)| (a - b).powi(2)).sum::<f64>().sqrt()
}

fn load_dbpedia_vectors(file_path: &str, limit: usize) -> Result<Vec<(String, Vec<f64>)>, PolarsError> {
    let df = ParquetReader::new(std::fs::File::open(file_path)?)
        .finish()?
        .lazy()
        .limit(limit as u32)
        .collect()?;

    let ids = df.column("_id")?.str()?;
    let embeddings = df.column("openai")?.list()?;

    let mut vectors = Vec::new();
    for (i, (_id, embedding)) in ids.into_iter().zip(embeddings.into_iter()).enumerate() {
        let f64_series = embedding
            .unwrap()
            .cast(&DataType::Float64)
            .unwrap();
        let chunked = f64_series.f64().unwrap();
        let vector: Vec<f64> = chunked.into_no_null_iter().collect();
        vectors.push((i.to_string(), vector));
    }
    Ok(vectors)
}
