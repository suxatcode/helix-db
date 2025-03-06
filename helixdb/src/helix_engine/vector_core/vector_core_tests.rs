use heed3::{Env, EnvOpenOptions};
use rand::{rngs::StdRng, Rng, SeedableRng, prelude::SliceRandom};
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

#[test]
fn test_vector_core_creation() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let hnsw = VectorCore::new(&env, &mut txn, None);
    assert!(hnsw.is_ok());

    let hnsw = VectorCore::new(&env, &mut txn, Some(Default::default()));
    assert!(hnsw.is_ok());

    txn.commit().unwrap();
}

#[test]
fn test_get_new_level_distribution() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let hnsw = VectorCore::new(&env, &mut txn, None);
    assert!(hnsw.is_ok());

    let hnsw = VectorCore::new(&env, &mut txn, Some(Default::default()));
    assert!(hnsw.is_ok());

    txn.commit().unwrap();
}


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
