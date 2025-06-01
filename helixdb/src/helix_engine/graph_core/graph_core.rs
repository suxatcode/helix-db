/*
use crate::helix_engine::storage_core::storage_core::HelixGraphStorage;
use crate::helix_engine::storage_core::storage_methods::StorageMethods;
use crate::helix_engine::types::GraphError;
use crate::props;
use crate::protocol::filterable::{Filterable, FilterableType};
use crate::protocol::remapping::{Remapping, ResponseRemapping};
use std::collections::HashMap;
use std::ops::Deref;
use std::str;
use std::sync::{Arc, RwLock};

use super::config::VectorConfig;
use crate::helixc::parser::helix_parser::{
    BooleanOp, Expression, GraphStep, HelixParser, IdType, Source, StartNode, Statement, Step,
    Traversal,
};
use crate::protocol::traversal_value::TraversalValue;
use crate::protocol::{
    items::{Edge, Node},
    return_values::ReturnValue,
    value::Value,
};

use crate::helix_engine::graph_core::config::Config;

// TODO: don't need this anymore
#[derive(Debug)]
pub enum QueryInput {
    StringValue { value: String },
    IntegerValue { value: i32 },
    FloatValue { value: f64 },
    BooleanValue { value: bool },
}

pub struct HelixGraphEngine { // TODO: is there a reason for this?
    pub storage: Arc<HelixGraphStorage>,
}

pub struct HelixGraphEngineOpts {
    pub path: String,
    pub config: Config,
}

impl HelixGraphEngine {
    pub fn new(path: Option<String>, config: Config) -> Result<HelixGraphEngine, GraphError> {
        let path_str = path.unwrap_or_else(String::new);
        let storage = match HelixGraphStorage::new(&path_str, config) {
            Ok(db) => Arc::new(db),
            Err(err) => return Err(err),
        };
        Ok(HelixGraphEngine { storage })
    }

    pub fn query(&self, query: String, params: Vec<QueryInput>) -> Result<String, GraphError> {
        Ok(String::new())
    }
}
*/

