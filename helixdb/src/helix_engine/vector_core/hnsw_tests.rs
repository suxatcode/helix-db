use heed3::{Env, EnvOpenOptions};
use rand::{rngs::StdRng, Rng, SeedableRng, prelude::SliceRandom};
use crate::helix_engine::vector_core::{vector::HVector, vector_core::{HNSWConfig, VectorCore}};
use polars::prelude::*;
use std::collections::HashSet;
use std::fs::{self, File};
use std::time::Instant;

fn setup_temp_env() -> Env {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    unsafe {
        EnvOpenOptions::new()
            .map_size(2 * 1024 * 1024 * 1024) // 2GB
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

fn calc_ground_truths(vectors: Vec<HVector>, query_vectors: Vec<(String, Vec<f64>)>, k: usize) -> Vec<Vec<String>> {
    let mut ground_truths = Vec::new();

    for (_, query) in query_vectors {
        let hquery = HVector::from_slice("".to_string(), 0, query.to_vec());
        let mut distances: Vec<(String, f64)> = vectors
            .iter()
            .map(|hvector| {
                let vector = hvector;
                (vector.get_id().to_string(), vector.distance_to(&hquery))
            })
            .collect();
        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let top_k: Vec<String> = distances.iter().take(k).map(|(id, _)| id.clone()).collect();
        ground_truths.push(top_k);
    }

    ground_truths
}

fn load_dbpedia_vectors(limit: usize) -> Result<Vec<(String, Vec<f64>)>, PolarsError> {
    // from data/ dir (https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M)
    if limit > 1_000_000 {
        return Err(PolarsError::OutOfBounds(
            "can't load more than 1,000,000 vecs from this dataset".into(),
        ));
    }

    //let data_dir = "./src/helix_engine/data/";
    let data_dir = "../data/";
    let mut all_vectors = Vec::new();
    let mut total_loaded = 0;

    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();

        // println!("loading entry: {:?}", path);

        if path.is_file() && path.extension().map_or(false, |ext| ext == "parquet") {
            let df = ParquetReader::new(File::open(&path)?)
                .finish()?
                .lazy()
                .limit((limit - total_loaded) as u32)
                .collect()?;

            let ids = df.column("_id")?.str()?;
            let embeddings = df.column("openai")?.list()?;

            for (_id, embedding) in ids.into_iter().zip(embeddings.into_iter()) {
                if total_loaded >= limit {
                    break;
                }

                let embedding = embedding.unwrap();
                let f64_series = embedding.cast(&DataType::Float64).unwrap();
                let chunked = f64_series.f64().unwrap();
                let vector: Vec<f64> = chunked.into_no_null_iter().collect();

                all_vectors.push((
                    _id.unwrap().to_string(),
                    vector
                ));

                total_loaded += 1;
            }

            if total_loaded >= limit {
                break;
            }
        }
    }

    Ok(all_vectors)
}

// cargo --release test test_name -- --nocapture

#[test]
fn test_recall_precision_real_data() {
    let n_base = 50_000;
    let dims = 1536;
    let vectors = load_dbpedia_vectors(n_base).unwrap();
    println!("loaded {} vectors", vectors.len());

    let n_query = 5_000;
    let mut rng = rand::rng();
    let mut shuffled_vectors = vectors.clone();
    shuffled_vectors.shuffle(&mut rng);
    let base_vectors = &shuffled_vectors[..n_base - n_query];
    let query_vectors = &shuffled_vectors[n_base - n_query..];

    println!("num of base vecs: {}", base_vectors.len());
    println!("num of query vecs: {}", query_vectors.len());

    let k = 10;

    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let mut all_hvectors: Vec<HVector> = Vec::new();

    let mut total_insertion_time = std::time::Duration::from_secs(0);
    let index = VectorCore::new(
        &env,
        &mut txn,
        HNSWConfig::new_with_params(n_base, 16, 256, 800),
    ).unwrap();

    for (i, (id, data)) in vectors.iter().enumerate() {
        let start_time = Instant::now();
        let vec = index.insert(&mut txn, data, None).unwrap();
        let time = start_time.elapsed();
        println!("{} => loading in {} ms, vector: {}", i, time.as_millis(), id);
        total_insertion_time += time;

        all_hvectors.push(vec);
    }
    txn.commit().unwrap();
    let txn = env.read_txn().unwrap();
    println!("{:?}", index.config);

    println!("total insertion time: {:.2?} seconds", total_insertion_time.as_secs_f64());
    println!(
        "average insertion time per vec: {:.2?} milliseconds",
        total_insertion_time.as_millis() as f64 / n_base as f64
    );

    println!("calculating ground truth distances...");
    let ground_truth = calc_ground_truths(all_hvectors, query_vectors.to_vec(), k);

    println!("searching and comparing...");
    let test_id = format!("k = {} with {} queries", k, n_query);

    let mut total_recall = 0.0;
    let mut total_precision = 0.0;

    let mut total_search_time = std::time::Duration::from_secs(0);

    for ((_, query), gt) in query_vectors.iter().zip(ground_truth.iter()) {
        let start_time = Instant::now();
        let results = index.search(&txn, query, k).unwrap();
        let search_duration = start_time.elapsed();
        total_search_time += search_duration;

        let result_indices: HashSet<String> = results.into_iter()
            .map(|hvector| hvector.get_id().to_string())
            .collect();

        let gt_indices: HashSet<String> = gt.iter().cloned().collect();
        //println!("gt: {:?}\nresults: {:?}\n", gt_indices, result_indices);
        let true_positives = result_indices.intersection(&gt_indices).count();

        let recall: f64 = true_positives as f64 / gt_indices.len() as f64;
        let precision:f64 = true_positives as f64 / result_indices.len() as f64;

        total_recall += recall;
        total_precision += precision;
    }

    println!("total search time: {:.2?} seconds", total_search_time.as_secs_f64());
    println!(
        "average search time per query: {:.2?} milliseconds",
        total_search_time.as_millis() as f64 / n_query as f64
    );

    total_recall = total_recall / n_query as f64;
    total_precision = total_precision / n_query as f64;
    println!("{}: avg. recall: {:.4?}, avg. precision: {:.4?}", test_id, total_recall, total_precision);
    assert!(total_recall >= 0.8, "recall not high enough!");
}

#[test]
fn test_insert_speed() {
    let n_base = 10000;
    let dims = 1536;
    let vectors = load_dbpedia_vectors(n_base).unwrap();
    println!("loaded {} vectors", vectors.len());

    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let mut total_insertion_time = std::time::Duration::from_secs(0);
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(n_base)).unwrap();
    for (i, (id, data)) in vectors.iter().enumerate() {
        let start_time = Instant::now();
        index.insert(&mut txn, data, Some(id.clone())).unwrap();
        let time = start_time.elapsed();
        println!("{} => loading in {} ms, vector: {}", i, time.as_millis(), id);
        total_insertion_time += time;
    }
    txn.commit().unwrap();

    println!("total insertion time: {:.2?} seconds", total_insertion_time.as_secs_f64());
    println!(
        "average insertion time per vec: {:.2?} milliseconds",
        total_insertion_time.as_millis() as f64 / n_base as f64
    );
    assert!(false);
}
