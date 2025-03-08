use heed3::{Env, EnvOpenOptions};
use rand::{rngs::StdRng, Rng, SeedableRng, prelude::SliceRandom};
use super::vector::HVector;
use crate::helix_engine::vector_core::vector_core::{HNSWConfig, VecConfig, VectorCore};
use polars::prelude::*;
use std::collections::HashSet;

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

fn load_dbpedia_vectors(limit: usize) -> Result<Vec<(String, Vec<f64>)>, PolarsError> {
    // from data/ dir (https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M)
    let file_path = "../data/train-00000-of-00026-3c7b99d1c7eda36e.parquet";
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

#[test]
fn test_recall_dpedia_entities_30k() {
    let n_base = 30_000;
    let vectors = load_dbpedia_vectors(n_base).unwrap();
    println!("loaded {} vectors", vectors.len());

    let n_query = 5000;
    let mut rng = rand::rng();
    let mut shuffled_vectors = vectors.clone();
    shuffled_vectors.shuffle(&mut rng);
    let base_vectors = &shuffled_vectors[..n_base - n_query];
    let query_vectors = &shuffled_vectors[n_base - n_query..];

    println!("num of base vecs: {}", base_vectors.len());
    println!("num of query vecs: {}", query_vectors.len());

    let dims = 1536;
    let ks = [10, 15, 30, 50, 100];
    let k = 100;

    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let hnsw_config = HNSWConfig::optimized(n_base);
    println!("hnsw config: {:?}", hnsw_config);
    let index = VectorCore::new(&env, &mut txn, dims, Some(hnsw_config), None).unwrap();

    let mut inserted_ids = Vec::new();
    for (_id, data) in base_vectors.iter().take(10) {
        let inserted_vec = index.insert(&mut txn, data).unwrap();
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
        let top_k: Vec<String> = distances.iter().take(100).map(|(i, _)| i.clone()).collect();
        ground_truth.push(top_k);
    }

    println!("searching and comparing...");
    //for k in ks {
        let test_id = format!("recall@{} with {} queries", k, n_query);

        let mut total_recall = 0.0;

        for ((_, query), gt) in query_vectors.iter().zip(ground_truth.iter()) {
            let results = index.search(&txn, query, k).unwrap();
            let result_indices: HashSet<String> = results.into_iter()
                .map(|hvector| hvector.get_id().to_string())
                .collect();

            let gt_indices: HashSet<String> = gt.iter().cloned().collect();
            println!("gt: {:?}\nresults: {:?}", gt_indices, result_indices);
            let intersection = result_indices.intersection(&gt_indices).count();
            let recall = intersection as f64 / gt_indices.len() as f64;

            total_recall += recall;
        }

        total_recall = total_recall / n_query as f64;
        println!("{}: {}", test_id, total_recall);
        //assert!(total_recall >= 0.8, "precision not high enough!");
    //}
}

//fn test_precision_with_fake_data() {
//}

#[test]
fn test_vector_core_creation() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let dims = 0;
    let index = VectorCore::new(&env, &mut txn, dims, None, None);
    assert!(index.is_ok());

    let index = VectorCore::new(&env, &mut txn, dims, Some(HNSWConfig::default()), Some(VecConfig::default()));
    assert!(index.is_ok());

    txn.commit().unwrap();
}

#[test]
fn test_get_new_level() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let dims = 0;
    let _index = VectorCore::new(&env, &mut txn, dims, None, None);
    txn.commit().unwrap();
}

// TODO:

//test get_new_level

//get_entry_point

//set_entry_point

//get_vector

//put_vector

//get_neighbors

//set_neighbours

//select_neighbors

//search_level

//search

//insert

//get_all_vectors
