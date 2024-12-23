use crate::types::GraphError;
use crate::HelixGraphStorage;
use std::str;
use std::sync::{Arc, Mutex};

use super::traversal::TraversalBuilder;
use serde_json::json;
pub struct HelixGraphEngine {
    pub storage: HelixGraphStorage,
}

impl HelixGraphEngine {
    pub fn new(path: &str) -> Result<HelixGraphEngine, GraphError> {
        let storage = match HelixGraphStorage::new(path) {
            Ok(db) => db,
            Err(err) => return Err(err),
        };
        Ok(Self { storage })
    }

    pub fn print_result_as_json(&self, traversal: &TraversalBuilder) {
        let current_step = &traversal.current_step;
        let json_result = json!(current_step);
        println!("{}", json_result.to_string());
    }

    pub fn print_result_as_pretty_json(&self, traversal: &TraversalBuilder) {
        let current_step = &traversal.current_step;
        let json_result = json!(current_step);
        println!("{}", serde_json::to_string_pretty(&json_result).unwrap());
    }

    /// implement error for this function
    pub fn result_to_json(&self, traversal: &TraversalBuilder) -> Vec<u8> {
        let current_step = &traversal.current_step;
        let mut json_string = serde_json::to_string(current_step).unwrap();
        json_string.push_str("\n");
        json_string.into_bytes()
    }
}
