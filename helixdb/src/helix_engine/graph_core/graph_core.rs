use crate::helix_engine::types::GraphError;
use crate::helix_gateway::mcp::mcp::{McpBackend, McpConnections};
use crate::helix_storage::{lmdb_storage::LmdbStorage, Storage};
use crate::props;
use crate::protocol::filterable::{Filterable, FilterableType};
use crate::protocol::remapping::{Remapping, ResponseRemapping};
use std::collections::HashMap;
use std::ops::Deref;
use std::str;
use std::sync::{Arc, Mutex, RwLock};

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

#[derive(Debug)]
pub enum QueryInput {
    StringValue { value: String },
    IntegerValue { value: i32 },
    FloatValue { value: f64 },
    BooleanValue { value: bool },
}

pub struct HelixGraphEngine<S: Storage + ?Sized> {
    pub storage: Arc<S>,
    pub mcp_backend: Option<Arc<McpBackend>>,
    pub mcp_connections: Option<Arc<Mutex<McpConnections>>>,
}

pub struct HelixGraphEngineOpts {
    pub path: String,
    pub config: Config,
}

impl HelixGraphEngineOpts {
    pub fn default() -> Self {
        Self {
            path: String::new(),
            config: Config::default(),
        }
    }
    pub fn with_path(path: String) -> Self {
        Self {
            path,
            config: Config::default(),
        }
    }
}

impl HelixGraphEngine<LmdbStorage> {
    pub fn new(opts: HelixGraphEngineOpts) -> Result<HelixGraphEngine<LmdbStorage>, GraphError> {
        let should_use_mcp = opts.config.mcp;
        let storage = Arc::new(LmdbStorage::new(opts.path.as_str(), opts.config)?);

        let (mcp_backend, mcp_connections) = if should_use_mcp {
            let mcp_backend = Arc::new(McpBackend::new(storage.clone()));
            let mcp_connections = Arc::new(Mutex::new(McpConnections::new()));
            (Some(mcp_backend), Some(mcp_connections))
        } else {
            (None, None)
        };
        Ok(Self {
            storage,
            mcp_backend,
            mcp_connections,
        })
    }
}

impl<S: Storage + 'static> HelixGraphEngine<S> {
    pub fn query(&self, query: String, params: Vec<QueryInput>) -> Result<String, GraphError> {
        Ok(String::new())
    }
}
