use bindings::js_binding::HelixJS;
use chrono::Utc;
use helix_engine::{
    graph_core::graph_core::HelixGraphEngine,
    props,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use helix_gateway::{
    router::router::{HandlerFn, HandlerInput, HandlerSubmission, HelixRouter, RouterError},
    GatewayOpts, HelixGateway,
};
use inventory;
use protocol::{request::Request, response::Response};
use rand::Rng;
use std::{collections::HashMap, sync::Arc};

use helixc::parser::helix_parser::HelixParser::{self, Expression, Source}

pub mod bindings;

uniffi::include_scaffolding!("helix");

pub struct HelixEmbedded {
    graph: Arc<HelixGraphEngine>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Input {
    String(String),
    Integer(i32),
    Float(f64),
}

impl HelixEmbedded {
    pub fn new(user: String) -> Result<Self, GraphError> {
        let home_dir = dirs::home_dir().ok_or(GraphError::New(
            "Unable to determine home directory".to_string(),
        ))?;
        let path = format!("{}/.helix/graph_data/{}", home_dir.display(), user);
        println!("Path: {:?}", path);
        let graph = Arc::new(HelixGraphEngine::new(path.as_str()).unwrap());
        Ok(Self { graph })
    }

    pub fn query(&self, query: String, params: Vec<Input>) -> Result<String, HelixLiteError> {
        let ast = HelixParser::parse_source(query.to_str());
        let return_vals: HashMap<String, String> = HashMap::new();
        let vars: HashMap<String, Input> = HashMap::new();

        for query in ast.queries {
            for stmt in query.statements {
                match stmt.value {
                    Expression::Traversal(_) |  Expression::Exists(_) => {
                        // build traversal based on steps with traversal builder
                        // initialise from start node
                        // step through all steps and execute.
                    }
                    _ => {
                        // insert variable to hashmap
                        let var = match stmt.value {
                            StringLiteral(value) => Input::String(value),
                            NumberLiteral(value) => Input::Integer(value),
                            BooleanLiteral(value) => Input::Boolean(value),
                        }
                        vars.insert(stmt.variable, var)
                    }
                }
            }
        }
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


