use heed3::{Env, EnvOpenOptions};
use std::collections::HashSet;
use rand::{rngs::StdRng, Rng, SeedableRng, prelude::SliceRandom};
use polars::prelude::*;

use super::hnsw::HNSW;
use super::vector::HVector;
use super::vector_core::HNSWConfig;
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

#[test]
fn test_hnsw_creation() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let hnsw = VectorCore::new(&env, &mut txn, None);
    assert!(hnsw.is_ok());

    let hnsw = VectorCore::new(&env, &mut txn, Some(Default::default()));
    assert!(hnsw.is_ok());

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_insert_single() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let hnsw = VectorCore::new(&env, &mut txn, None).unwrap();

    let id = "test_vec";
    let data = vec![1.0, 2.0, 3.0];

    let result = hnsw.insert(&mut txn, &data);
    assert!(result.is_ok());

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_insert_single_reduced_dims() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let hnsw_config = HNSWConfig::with_dim_reduce(3, None);
    let hnsw = VectorCore::new(&env, &mut txn, Some(hnsw_config)).unwrap();

    let id = "test_vec";
    let data = vec![1.0, 2.0, 3.0];

    let result = hnsw.insert(&mut txn, &data);
    assert!(result.is_ok());

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_search_empty() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let hnsw = VectorCore::new(&env, &mut txn, None).unwrap();

    let query = HVector::new("query".to_string(), vec![1.0, 2.0, 3.0]);
    let results = hnsw.search(&txn, &query, 5);

    assert!(results.is_ok());
    assert!(results.unwrap().is_empty());

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_search_empty_reduced_dims() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let hnsw_config = HNSWConfig::with_dim_reduce(3, None);
    let hnsw = VectorCore::new(&env, &mut txn, Some(hnsw_config)).unwrap();

    let query = HVector::new("query".to_string(), vec![1.0, 2.0, 3.0]);
    let results = hnsw.search(&txn, &query, 5);

    assert!(results.is_ok());
    assert!(results.unwrap().is_empty());

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_insert_and_search() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let hnsw = VectorCore::new(&env, &mut txn, None).unwrap();

    let mut vectors = generate_random_vectors(1000, 10, 42);

    for (i, (_, data)) in vectors.clone().iter().enumerate() {
        let result = hnsw.insert(&mut txn, data);
        assert!(result.is_ok());
        let id = result.unwrap();
        vectors[i].0 = id;
    }
    let query_id = &vectors[0].0;
    let query_data = &vectors[0].1;
    let query = HVector::new(query_id.clone(), query_data.clone());

    let results = hnsw.search(&txn, &query, 24).unwrap();

    assert!(!results.is_empty());
    assert_eq!(results[0].0, *query_id);
    assert!(results[0].1 < 0.001);
    assert!(results.len() == 24);
    txn.commit().unwrap();
}

#[test]
fn test_hnsw_insert_and_search_reduced_dims() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let dim = 10;
    let hnsw_config = HNSWConfig::with_dim_reduce(dim, None);
    let hnsw = VectorCore::new(&env, &mut txn, Some(hnsw_config)).unwrap();

    let mut vectors = generate_random_vectors(1000, dim, 42);

    for (i, (_, data)) in vectors.clone().iter().enumerate() {
        let result = hnsw.insert(&mut txn, data);
        assert!(result.is_ok());
        let id = result.unwrap();
        vectors[i].0 = id;
    }

    let query_id = &vectors[0].0;
    let query_data = &vectors[0].1;
    let query = HVector::new(query_id.clone(), query_data.clone());

    let results = hnsw.search(&txn, &query, 24).unwrap();

    assert!(!results.is_empty());
    assert_eq!(results[0].0, *query_id);
    assert!(results[0].1 < 0.001);

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_accuracy() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let hnsw = VectorCore::new(&env, &mut txn, None).unwrap();

    let mut vectors = Vec::new();

    for cluster in 0..3 {
        let mut base = vec![0.0; 5];
        base[cluster] = 10000.0;

        for i in 0..5 {
            let id = format!("cluster_{}_vec_{}", cluster, i);
            let mut data = base.clone();

            for j in 0..data.len() {
                data[j] += i as f64 * 0.01;
            }

            vectors.push((id, data));
        }
    }

    for (i, (_, data)) in vectors.clone().iter().enumerate() {
        let result = hnsw.insert(&mut txn, data);
        assert!(result.is_ok());
        let id = result.unwrap();
        vectors[i].0 = id;
    }

    for cluster in 0..3 {
        let query_idx = cluster * 5;
        let query_id = &vectors[query_idx].0;
        let query_data = &vectors[query_idx].1;
        let query = HVector::new(query_id.clone(), query_data.clone());

        let results = hnsw.search(&txn, &query, 5).unwrap();
        assert!(
            !results.is_empty(),
            "No results found for query: {}",
            query_id
        );
        assert_eq!(results[0].0, *query_id);
    }

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_accuracy_reduced_dims() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let dim = 5;

    let hnsw_config = HNSWConfig::with_dim_reduce(dim, None);
    let hnsw = VectorCore::new(&env, &mut txn, Some(hnsw_config)).unwrap();

    let mut vectors = Vec::new();

    for cluster in 0..3 {
        let mut base = vec![0.0; dim];
        base[cluster] = 10000.0;

        for i in 0..5 {
            let id = format!("cluster_{}_vec_{}", cluster, i);
            let mut data = base.clone();

            for j in 0..data.len() {
                data[j] += i as f64 * 0.01;
            }

            vectors.push((id, data));
        }
    }

    for (i, (_, data)) in vectors.clone().iter().enumerate() {
        let result = hnsw.insert(&mut txn, data);
        assert!(result.is_ok());
        let id = result.unwrap();
        vectors[i].0 = id;
    }

    for cluster in 0..3 {
        let query_idx = cluster * 5;
        let query_id = &vectors[query_idx].0;
        let query_data = &vectors[query_idx].1;
        let query = HVector::new(query_id.clone(), query_data.clone());

        let results = hnsw.search(&txn, &query, 5).unwrap();
        assert!(
            !results.is_empty(),
            "No results found for query: {}",
            query_id
        );
        println!("results: {:?}", results[0]);
        println!("query_id: {:?}", query_id);
        assert_eq!(results[0].0, *query_id);
        println!("results: {:?}", results);
        assert_eq!(results.len(), 5);
    }

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_persistence() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    let dim = 10;
    let mut vectors = generate_random_vectors(50, dim, 42);


    {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(10)
                .open(path)
                .unwrap()
        };

        let mut txn = env.write_txn().unwrap();
        let hnsw = VectorCore::new(&env, &mut txn, None).unwrap();

        for (i, (_, data)) in vectors.clone().iter().enumerate() {
            let result = hnsw.insert(&mut txn, data);
            assert!(result.is_ok());
            let id = result.unwrap();
            vectors[i].0 = id;
        }

        txn.commit().unwrap();
    }

    {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(10)
                .open(path)
                .unwrap()
        };

        let mut init_txn = env.write_txn().unwrap();
        let hnsw = VectorCore::new(&env, &mut init_txn, None).unwrap();
        init_txn.commit().unwrap();

        let txn = env.read_txn().unwrap();
        let query_id = vectors[0].0.clone();
        let query_data = vectors[0].1.clone();
        let query = HVector::new(query_id.clone(), query_data);
        let results = hnsw.search(&txn, &query, 5).unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].0, query_id);
        assert!(results[0].1 < 0.001);
    }
}

#[test]
fn test_hnsw_persistence_reduced_dims() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    let dim = 10;
    let mut vectors = generate_random_vectors(50, dim, 42);


    {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(10)
                .open(path)
                .unwrap()
        };

        let mut txn = env.write_txn().unwrap();
        let hnsw_config = HNSWConfig::with_dim_reduce(dim, None);
        let hnsw = VectorCore::new(&env, &mut txn, Some(hnsw_config)).unwrap();

        for (i, (_, data)) in vectors.clone().iter().enumerate() {
            let result = hnsw.insert(&mut txn, data);
            assert!(result.is_ok());
            let id = result.unwrap();
            vectors[i].0 = id;
        }

        txn.commit().unwrap();
    }

    {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(10)
                .open(path)
                .unwrap()
        };

        let mut init_txn = env.write_txn().unwrap();
        let hnsw_config = HNSWConfig::with_dim_reduce(dim, None);
        let hnsw = VectorCore::new(&env, &mut init_txn, Some(hnsw_config)).unwrap();
        init_txn.commit().unwrap();

        let txn = env.read_txn().unwrap();
        let query_id = vectors[0].0.clone();
        let query_data = vectors[0].1.clone();
        let query = HVector::new(query_id.clone(), query_data);
        let results = hnsw.search(&txn, &query, 5).unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].0, query_id);
        assert!(results[0].1 < 0.001);
    }
}

#[test]
fn test_hnsw_large_scale() {
    let env = setup_temp_env();

    let config = HNSWConfig {
        m: 16,
        m_max: 32,
        ef_construction: 500,
        max_elements: 10_000,
        ml_factor: 1.0 / std::f64::consts::LN_2,
        distance_multiplier: 1.0,
        target_dimension: None,
    };

    let mut txn = env.write_txn().unwrap();
    let hnsw = VectorCore::new(&env, &mut txn, Some(config)).unwrap();

    let mut vectors = generate_random_vectors(100, 10, 42);

    for (i, (_, data)) in vectors.clone().iter().enumerate() {
        let result = hnsw.insert(&mut txn, data);
        assert!(result.is_ok());
        let id = result.unwrap();
        vectors[i].0 = id;
    }

    let query_indices = vec![0, 10, 20];

    for &idx in &query_indices {
        let query_id = &vectors[idx].0;
        let query_data = &vectors[idx].1;
        let query = HVector::new(query_id.clone(), query_data.clone());

        let results = hnsw.search(&txn, &query, 10).unwrap();
        println!("results: {:?}", results);
        assert!(
            !results.is_empty(),
            "No results found for query vector {}",
            query_id
        );

        println!(
            "Query: {}, Found {} results. First result: {} with distance {}",
            query_id,
            results.len(),
            results[0].0,
            results[0].1
        );
    }

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_large_scale_reduced_dims() {
    let env = setup_temp_env();

    let dim = 10;
    let config = HNSWConfig {
        m: 16,
        m_max: 32,
        ef_construction: 500,
        max_elements: 10_000,
        ml_factor: 1.0 / std::f64::consts::LN_2,
        distance_multiplier: 1.0,
        target_dimension: Some(HNSWConfig::calc_target_dim(dim)),
    };

    let mut txn = env.write_txn().unwrap();
    let hnsw = VectorCore::new(&env, &mut txn, Some(config)).unwrap();

    let mut vectors = generate_random_vectors(100, dim, 42);

    for (i, (_, data)) in vectors.clone().iter().enumerate() {
        let result = hnsw.insert(&mut txn, data);
        assert!(result.is_ok());
        let id = result.unwrap();
        vectors[i].0 = id;
    }

    let query_indices = vec![0, 10, 20];

    for &idx in &query_indices {
        let query_id = &vectors[idx].0;
        let query_data = &vectors[idx].1;
        let query = HVector::new(query_id.clone(), query_data.clone());

        let results = hnsw.search(&txn, &query, 10).unwrap();

        assert!(
            !results.is_empty(),
            "No results found for query vector {}",
            query_id
        );

        println!(
            "Query: {}, Found {} results. First result: {} with distance {}",
            query_id,
            results.len(),
            results[0].0,
            results[0].1
        );
    }

    txn.commit().unwrap();
}

#[test]
fn test_hnsw_edge_cases() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let hnsw = VectorCore::new(&env, &mut txn, None).unwrap();

    let result = hnsw.insert(&mut txn, &[]);
    assert!(result.is_ok());

    let large_vec: Vec<f64> = (0..1000).map(|i| i as f64).collect();
    let result = hnsw.insert(&mut txn, &large_vec);
    assert!(result.is_ok());

    let result1 = hnsw.insert(&mut txn, &[1.0, 2.0, 3.0]);
    assert!(result1.is_ok());

    let result2 = hnsw.insert(&mut txn, &[4.0, 5.0, 6.0]);
    assert!(result2.is_ok());

    let query = HVector::new("query".to_string(), vec![4.0, 5.0, 6.0]);
    let results = hnsw.search(&txn, &query, 1).unwrap();

    assert!(!results.is_empty());

    txn.commit().unwrap();
}

// TODO: remove? bc not using diff dims
#[test]
fn test_hnsw_different_dimensions() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let hnsw = VectorCore::new(&env, &mut txn, None).unwrap();

    hnsw.insert(&mut txn, &[1.0, 2.0]).unwrap();
    hnsw.insert(&mut txn, &[1.0, 2.0, 3.0]).unwrap();
    hnsw.insert(&mut txn, &[1.0, 2.0, 3.0, 4.0]).unwrap();

    let query_2d = HVector::new("query".to_string(), vec![1.0, 2.0]);
    let results_2d = hnsw.search(&txn, &query_2d, 3).unwrap();

    let query_3d = HVector::new("query".to_string(), vec![1.0, 2.0, 3.0]);
    let results_3d = hnsw.search(&txn, &query_3d, 3).unwrap();

    assert!(!results_2d.is_empty());
    assert!(!results_3d.is_empty());
    println!("Results 2d: {:?}", results_2d);
    txn.commit().unwrap();
}

/// test to run alone for testing with actual data (for now)
#[test]
fn test_accuracy_with_dbpedia_openai() {
    // cargo test test_accuracy_with_dbpedia_openai -- --nocapture
    // from data/ dir (https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M)
    let file_path = "../data/train-00000-of-00026-3c7b99d1c7eda36e.parquet";
    let n_base = 10_000; // Number of base vectors (subset) (increase for better testing)
    let n_query = 10;   // Number of query vectors
    let k = 10;          // Number of neighbors to retrieve

    let vectors = load_dbpedia_vectors(file_path, n_base).unwrap();
    println!("Loaded {} vectors", vectors.len());

    // split into base and query sets
    let mut rng = rand::rng();
    let mut shuffled_vectors = vectors.clone();
    shuffled_vectors.shuffle(&mut rng);
    let base_vectors = &shuffled_vectors[..n_base - n_query];
    let query_vectors = &shuffled_vectors[n_base - n_query..];

    println!("num of base vecs: {}", base_vectors.len());
    println!("num of query vecs: {}", query_vectors.len());

    // compute ground truth using exact search (euclidean_distance)
    let mut ground_truth = Vec::new();
    for (_, query) in query_vectors {
        let mut distances: Vec<(String, f64)> = base_vectors // calcing euc for every other, our sol: hnsw
            .iter()
            .map(|(id, v)| (id.clone(), euclidean_distance(query, v))) // TODO: find better sol for clone() here
            .collect();
        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let top_k: Vec<String> = distances.iter().take(k).map(|(i, _)| i.clone()).collect(); // TODO: find better sol for clone() here
        ground_truth.push(top_k);
    }

    //let hnsw_config = HNSWConfig::with_dim_reduce(original_dim, None);
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let hnsw = VectorCore::new(&env, &mut txn, None).unwrap(); // TODO: rename to db
    for (id, data) in base_vectors.iter().take(10) {
        hnsw.insert(&mut txn, data).unwrap();
    }
    txn.commit().unwrap();
    let txn = env.read_txn().unwrap();

    // run queries and compute recall@k
    let mut total_recall = 0.0;
    for ((_, query), gt) in query_vectors.iter().zip(ground_truth.iter()) {
        let search_q = HVector::new("query".to_string(), query.clone());
        let results = hnsw.search(&txn, &search_q, k).unwrap();
        println!("results: {:?}", results);

        //println!("ground truth: {:?}, hnsw result: {:?}", gt, results);

        let result_indices: HashSet<String> = results.into_iter().map(|(id, _)| id).collect();
        let gt_indices: HashSet<String> = gt.iter().cloned().collect();
        let intersection = result_indices.intersection(&gt_indices).count();
        let recall = intersection as f64 / k as f64;
        total_recall += recall;
    }

    // well tuned vector dbs typically achieve recall rates between 0.8 and 0.99
    let average_recall = total_recall / n_query as f64;
    println!("Average recall@{}: {:.4}", k, average_recall);
    //assert!(average_recall > 0.8, "Recall too low: {}", average_recall);
}
