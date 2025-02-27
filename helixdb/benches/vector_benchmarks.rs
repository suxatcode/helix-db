use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use helixdb::helix_engine::{
    storage_core::{
        hnsw::{HNSW, HNSWConfig},
        vectors::HVector,
    },
    types::GraphError,
};
use heed3::{EnvOpenOptions, Env};
use rand::{rngs::StdRng, SeedableRng, Rng};
use std::{path::Path, time::Duration};
use tempfile::TempDir;

/// Creates a temporary environment for testing
fn setup_temp_env() -> (Env, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024) // 1GB
            .max_dbs(10)
            .open(path)
            .unwrap()
    };
    
    (env, temp_dir)
}

/// Generates random vectors with specified dimensions
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

/// Benchmark insertion of vectors with different dimensions
fn bench_vector_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_insertion");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    
    // Test with different vector dimensions
    for &dim in &[128, 1024, 4096, 8192] {
        // Number of vectors to insert per iteration
        let vectors_per_iter = 10;
        
        let id = BenchmarkId::new(format!("insert_{}vecs", vectors_per_iter), dim);
        group.bench_with_input(id, &dim, |b, &dim| {
            eprintln!("Benchmarking insertion of {} vectors with {} dimensions", vectors_per_iter, dim);
            
            b.iter_with_setup(
                || {
                    let (env, _temp_dir) = setup_temp_env();
                    let mut txn = env.write_txn().unwrap();
                    let hnsw = HNSW::new(&env, &mut txn, None).unwrap();
                    txn.commit().unwrap();
                    
                    let vectors = generate_random_vectors(100, dim, 42);
                    (env, hnsw, vectors)
                },
                |(env, mut hnsw, vectors)| {
                    let mut txn = env.write_txn().unwrap();
                    for (id, data) in vectors.iter().take(vectors_per_iter) {
                        hnsw.insert(&mut txn, id, data).unwrap();
                    }
                    txn.commit().unwrap();
                },
            );
        });
    }
    
    group.finish();
}

/// Benchmark search with different dimensions
fn bench_vector_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    
    // Test with different vector dimensions
    for &dim in &[128, 1024, 4096, 8192] {
        // Number of vectors in the index
        let index_size = 1000;
        // Number of queries per iteration
        let queries_per_iter = 10;
        
        let id = BenchmarkId::new(format!("search_{}q_{}idx", queries_per_iter, index_size), dim);
        group.bench_with_input(id, &dim, |b, &dim| {
            eprintln!("Benchmarking {} queries against index of {} vectors with {} dimensions", 
                     queries_per_iter, index_size, dim);
            
            // Setup: Create index with vectors
            let (env, _temp_dir) = setup_temp_env();
            let mut txn = env.write_txn().unwrap();
            let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
            
            let vectors = generate_random_vectors(index_size, dim, 42);
            eprintln!("Building index with {} vectors of {} dimensions...", vectors.len(), dim);
            
            for (id, data) in &vectors {
                hnsw.insert(&mut txn, id, data).unwrap();
            }
            txn.commit().unwrap();
            eprintln!("Index built successfully");
            
            // Create query vectors
            let query_vectors = generate_random_vectors(queries_per_iter, dim, 100);
            
            b.iter(|| {
                let txn = env.read_txn().unwrap();
                for (_, data) in &query_vectors {
                    let query = HVector::new("query".to_string(), data.clone());
                    let results = hnsw.search(&txn, &query, 10).unwrap();
                    black_box(results);
                }
            });
        });
    }
    
    group.finish();
}

/// Benchmark high throughput operations
fn bench_high_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_throughput");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(5);
    
    // Test with different numbers of vectors
    for &count in &[1000, 10000, 50000] {
        // Fixed dimension for throughput test
        let dim = 128;
        
        group.bench_with_input(BenchmarkId::new("build_index", count), &count, |b, &count| {
            eprintln!("Benchmarking building index with {} vectors of {} dimensions", count, dim);
            
            b.iter_with_setup(
                || {
                    let (env, _temp_dir) = setup_temp_env();
                    let vectors = generate_random_vectors(count, dim, 42);
                    (env, vectors)
                },
                |(env, vectors)| {
                    let mut txn = env.write_txn().unwrap();
                    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
                    
                    for (id, data) in &vectors {
                        hnsw.insert(&mut txn, id, data).unwrap();
                    }
                    txn.commit().unwrap();
                },
            );
        });
        
        // Benchmark batch search operations
        // Number of queries per iteration
        let queries_per_iter = 100;
        
        let id = BenchmarkId::new(format!("batch_search_{}q", queries_per_iter), count);
        group.bench_with_input(id, &count, |b, &count| {
            eprintln!("Benchmarking {} queries against index of {} vectors with {} dimensions", 
                     queries_per_iter, count, dim);
            
            // Setup: Create index with vectors
            let (env, _temp_dir) = setup_temp_env();
            let mut txn = env.write_txn().unwrap();
            let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
            
            let vectors = generate_random_vectors(count, dim, 42);
            eprintln!("Building index with {} vectors of {} dimensions...", vectors.len(), dim);
            
            for (id, data) in &vectors {
                hnsw.insert(&mut txn, id, data).unwrap();
            }
            txn.commit().unwrap();
            eprintln!("Index built successfully");
            
            // Create query vectors
            let query_vectors = generate_random_vectors(queries_per_iter, dim, 100);
            
            b.iter(|| {
                let txn = env.read_txn().unwrap();
                for (_, data) in &query_vectors {
                    let query = HVector::new("query".to_string(), data.clone());
                    let results = hnsw.search(&txn, &query, 10).unwrap();
                    black_box(results);
                }
            });
        });
    }
    
    group.finish();
}

/// Benchmark different HNSW configurations
fn bench_hnsw_configs(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_configs");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    
    // Test different ef_construction values
    let configs = vec![
        ("default", HNSWConfig::default()),
        ("high_precision", HNSWConfig {
            m: 32,
            m_max: 64,
            ef_construction: 500,
            max_elements: 1_000_000,
            ml_factor: 1.0 / std::f64::consts::LN_2,
            distance_multiplier: 1.0,
        }),
        ("high_performance", HNSWConfig {
            m: 8,
            m_max: 16,
            ef_construction: 100,
            max_elements: 1_000_000,
            ml_factor: 1.0 / std::f64::consts::LN_2,
            distance_multiplier: 1.0,
        }),
    ];
    
    // Fixed parameters for this benchmark
    let index_size = 1000;
    let dim = 1024;
    let queries_per_iter = 10;
    
    for (name, config) in configs {
        let id = BenchmarkId::new(format!("build_search_{}q_{}idx", queries_per_iter, index_size), name);
        group.bench_with_input(id, &config, |b, config| {
            eprintln!("Benchmarking config '{}' with {} vectors of {} dimensions and {} queries", 
                     name, index_size, dim, queries_per_iter);
            eprintln!("Config details: m={}, m_max={}, ef_construction={}", 
                     config.m, config.m_max, config.ef_construction);
            
            b.iter_with_setup(
                || {
                    let (env, _temp_dir) = setup_temp_env();
                    let vectors = generate_random_vectors(index_size, dim, 42);
                    let query_vectors = generate_random_vectors(queries_per_iter, dim, 100);
                    (env, vectors, query_vectors, config.clone())
                },
                |(env, vectors, query_vectors, config)| {
                    // Build index
                    let mut txn = env.write_txn().unwrap();
                    let mut hnsw = HNSW::new(&env, &mut txn, Some(config)).unwrap();
                    
                    for (id, data) in &vectors {
                        hnsw.insert(&mut txn, id, data).unwrap();
                    }
                    txn.commit().unwrap();
                    
                    // Search
                    let txn = env.read_txn().unwrap();
                    for (_, data) in &query_vectors {
                        let query = HVector::new("query".to_string(), data.clone());
                        let results = hnsw.search(&txn, &query, 10).unwrap();
                        black_box(results);
                    }
                },
            );
        });
    }
    
    group.finish();
}

/// Benchmark memory usage with high-dimensional vectors
fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(3); // Fewer samples due to high memory usage
    
    // Test with extremely high dimensions
    for &dim in &[4096, 8192] {
        // Fixed parameters for this benchmark
        let index_size = 100;
        let queries_per_iter = 1;
        
        let id = BenchmarkId::new(format!("high_dim_{}vecs_{}q", index_size, queries_per_iter), dim);
        group.bench_with_input(id, &dim, |b, &dim| {
            eprintln!("Benchmarking memory usage with {} vectors of {} dimensions", index_size, dim);
            let bytes_per_vector = dim * std::mem::size_of::<f64>();
            let estimated_raw_data_mb = (index_size * bytes_per_vector) as f64 / (1024.0 * 1024.0);
            eprintln!("Estimated raw vector data size: {:.2} MB (excluding index overhead)", estimated_raw_data_mb);
            
            b.iter_with_setup(
                || {
                    let (env, _temp_dir) = setup_temp_env();
                    let vectors = generate_random_vectors(index_size, dim, 42);
                    (env, vectors)
                },
                |(env, vectors)| {
                    let mut txn = env.write_txn().unwrap();
                    let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
                    
                    for (id, data) in &vectors {
                        hnsw.insert(&mut txn, id, data).unwrap();
                    }
                    txn.commit().unwrap();
                    
                    // Perform some searches to measure memory during active use
                    let txn = env.read_txn().unwrap();
                    let query = HVector::new("query".to_string(), vectors[0].1.clone());
                    let results = hnsw.search(&txn, &query, 10).unwrap();
                    black_box(results);
                },
            );
        });
    }
    
    group.finish();
}

/// Benchmark scaling with increasing vector counts
fn bench_vector_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_scaling");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(5);
    
    // Fixed dimension for this benchmark
    let dim = 256;
    
    // Test with exponentially increasing vector counts
    for &count in &[100, 1000, 10000, 50000] {
        let id = BenchmarkId::new("search_scaling", count);
        group.bench_with_input(id, &count, |b, &count| {
            eprintln!("Benchmarking search scaling with {} vectors of {} dimensions", count, dim);
            
            // Setup: Create index with vectors
            let (env, _temp_dir) = setup_temp_env();
            let mut txn = env.write_txn().unwrap();
            let mut hnsw = HNSW::new(&env, &mut txn, None).unwrap();
            
            let vectors = generate_random_vectors(count, dim, 42);
            eprintln!("Building index with {} vectors...", vectors.len());
            
            for (id, data) in &vectors {
                hnsw.insert(&mut txn, id, data).unwrap();
            }
            txn.commit().unwrap();
            eprintln!("Index built successfully");
            
            // Create 10 query vectors
            let query_vectors = generate_random_vectors(10, dim, 100);
            
            b.iter(|| {
                let txn = env.read_txn().unwrap();
                for (_, data) in &query_vectors {
                    let query = HVector::new("query".to_string(), data.clone());
                    let results = hnsw.search(&txn, &query, 10).unwrap();
                    black_box(results);
                }
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_vector_insertion,
    bench_vector_search,
    bench_high_throughput,
    bench_hnsw_configs,
    bench_memory_usage,
    bench_vector_scaling
);
criterion_main!(benches); 