use bindings::js_binding::HelixJS;
use chrono::Utc;
use helix_engine::{
    graph_core::{
        graph_core::{HelixGraphEngine, QueryInput},
        traversal::TraversalBuilder,
        traversal_steps::{SourceTraversalSteps, TraversalMethods, TraversalSteps},
    },
    props,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use helix_gateway::{
    router::router::{HandlerFn, HandlerInput, HandlerSubmission, HelixRouter, RouterError},
    GatewayOpts, HelixGateway,
};
use inventory;
use protocol::{
    count::Count, request::Request, response::Response, traversal_value::TraversalValue, Node,
    ReturnValue, Value,
};
use rand::Rng;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Instant,
};

use helixc::parser::{
    helix_parser::{
        BooleanOp, Expression, GraphStep, HelixParser, IdType, Source, StartNode, Statement, Step,
        Traversal,
    },
    parser_methods::ParserError,
};

pub mod bindings;

uniffi::include_scaffolding!("helix");
pub struct HelixEmbedded {
    graph: Arc<HelixGraphEngine>,
}



impl HelixEmbedded {
    pub fn new(user: String) -> Result<Self, HelixLiteError> {
        let home_dir = dirs::home_dir().ok_or(HelixLiteError::Default(
            "Unable to determine home directory".to_string(),
        ))?;
        let path = format!("{}/.helix/graph_data/{}", home_dir.display(), user);
        println!("Path: {:?}", path);
        let storage = match HelixGraphEngine::new(path.as_str()) {
            Ok(helix) => helix,
            Err(err) => return Err(HelixLiteError::from(err)),
        };
        let graph = Arc::new(storage);
        Ok(Self { graph })
    }


    pub fn query(&self, query: String, params: Vec<QueryInput>) -> Result<String, HelixLiteError> {
        self.graph.query(query, params).map_err(HelixLiteError::from)
    }
}

#[derive(Debug)]
pub enum HelixLiteError {
    Default(String),
}

impl std::fmt::Display for HelixLiteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HelixLiteError::Default(msg) => write!(f, "Graph error: {}", msg),
        }
    }
}

impl From<RouterError> for HelixLiteError {
    fn from(error: RouterError) -> Self {
        HelixLiteError::Default(error.to_string())
    }
}

impl From<GraphError> for HelixLiteError {
    fn from(error: GraphError) -> Self {
        HelixLiteError::Default(error.to_string())
    }
}

impl From<HelixLiteError> for GraphError {
    fn from(error: HelixLiteError) -> Self {
        GraphError::from(error.to_string())
    }
}

impl From<&'static str> for HelixLiteError {
    fn from(error: &'static str) -> Self {
        HelixLiteError::Default(error.to_string())
    }
}

impl From<String> for HelixLiteError {
    fn from(error: String) -> Self {
        HelixLiteError::Default(error)
    }
}

impl From<ParserError> for HelixLiteError {
    fn from(error: ParserError) -> Self {
        HelixLiteError::Default(error.to_string())
    }
}
