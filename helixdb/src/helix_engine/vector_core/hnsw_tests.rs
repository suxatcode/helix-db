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

/*
 * types of tests we need:
 * - speed
 * - accuarcy
 * - large scale
 */

/// test to run alone for testing with actual data (for now)
#[test]
fn test_accuracy_with_dbpedia_openai() {
    // cargo test test_accuracy_with_dbpedia_openai -- --nocapture
    // from data/ dir (https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M)
    let file_path = "../data/train-00000-of-00026-3c7b99d1c7eda36e.parquet";
    let n_base = 10_000; // number of base vectors (subset) (increase for better testing)
    let n_query = 1;     // number of query vectors
    let k = 10;          // number of neighbors to retrieve

    let vectors = load_dbpedia_vectors(file_path, n_base).unwrap();
    println!("loaded {} vectors", vectors.len());

    let mut rng = rand::rng();
    let mut shuffled_vectors = vectors.clone();
    shuffled_vectors.shuffle(&mut rng);
    let base_vectors = &shuffled_vectors[..n_base - n_query];
    let query_vectors = &shuffled_vectors[n_base - n_query..];

    println!("num of base vecs: {}", base_vectors.len());
    println!("num of query vecs: {}", query_vectors.len());

    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let db = VectorCore::new(&env, &mut txn, None).unwrap();
    let mut inserted_ids = Vec::new();
    for (_id, data) in base_vectors.iter().take(10) {
        let inserted_vec = db.insert(&mut txn, data).unwrap();
        inserted_ids.push((inserted_vec.get_id().to_string(), data.clone()));
    }
    txn.commit().unwrap();
    let txn = env.read_txn().unwrap();

    println!("calculating ground truth distances...");
    let mut ground_truth = Vec::new();
    for (_, query) in query_vectors {
        let mut distances: Vec<(String, f64)> = inserted_ids
            .iter()
            .map(|(id, v)| (id.clone(), euclidean_distance(query, v)))
            .collect();
        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let top_k: Vec<String> = distances.iter().take(k).map(|(i, _)| i.clone()).collect();
        ground_truth.push(top_k);
    }

    let mut total_recall = 0.0;
    for ((_, query), gt) in query_vectors.iter().zip(ground_truth.iter()) {
        let search_q = HVector::new("query".to_string(), query.clone());
        let results = db.search(&txn, &search_q, k).unwrap();
        println!("num results: {}, with k: {k}", results.len());

        let result_indices: HashSet<String> = results.into_iter()
            .map(|hvector| hvector.get_id().to_string())
            .collect();

        println!("ground truths: {:?}\nresults: {:?}", gt, result_indices);

        let gt_indices: HashSet<String> = gt.iter().cloned().collect();
        let intersection = result_indices.intersection(&gt_indices).count();
        let recall = intersection as f64 / k as f64;
        total_recall += recall;
    }

    // note: well tuned vector dbs typically achieve recall rates between 0.8 and 0.99
    let average_recall = total_recall / n_query as f64;
    println!("Average recall@{}: {:.4}", k, average_recall);
    //assert!(average_recall > 0.8, "Recall too low: {}", average_recall);
}
