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
use bindings::js_binding::HelixJS;


pub mod bindings;

uniffi::include_scaffolding!("helix");

pub struct HelixEmbedded {
    graph: Arc<HelixGraphEngine>,
    routes: HashMap<(String, String), HandlerFn>,
    router: HelixRouter,
}

impl HelixEmbedded {
    pub fn new(user: String) -> Result<Self, GraphError> {
        let home_dir = dirs::home_dir().ok_or(GraphError::New("Unable to determine home directory".to_string()))?;
        let path = format!("{}/.helix/graph_data/{}",home_dir.display(), user);
        println!("Path: {:?}", path);
        let graph = Arc::new(HelixGraphEngine::new(path.as_str()).unwrap());
        let routes: HashMap<(String, String), HandlerFn> = HashMap::from_iter(
            inventory::iter::<HandlerSubmission>
                .into_iter()
                .map(|submission| {
                    let handler = &submission.0;
                    let func: HandlerFn = Arc::new(move |input, response| (handler.func)(input, response));
                    ((
                        "post".to_ascii_uppercase().to_string(),
                        format!("/{}", handler.name.to_string()),
                    ), func)
                })
                .collect::<Vec<((String, String), HandlerFn)>>(),
        );
        let router = HelixRouter::new(Some(routes.clone()));
        Ok(Self {
            graph,
            routes,
            router
        })
    }

    pub fn get_routes(&self) -> &HashMap<(String, String), HandlerFn> {
        &self.routes
    }

    pub fn get_graph(&self) -> Arc<HelixGraphEngine> {
        Arc::clone(&self.graph)
    }

    pub fn query(&self, query_id: String, json_body: String) -> Result<String, HelixLiteError> {
        let request = Request {
            method: "POST".to_string(),
            headers: HashMap::new(),
            body: json_body.as_bytes().to_vec(),
            path: format!("/{}", query_id).to_string(),
        };
        
        println!("Querying with: {:?}", request);
        let mut response = Response::new();
        match self.router.handle(self.get_graph(), request, &mut response) {
            Ok(()) => Ok(String::from_utf8(response.body).unwrap()),
            Err(e) => Err(HelixLiteError::from(e))
        }
    }

    pub fn execute_query(&self, query: String, args: Vec<String>)->Result<String, HelixLiteError> {
        let parsed_query = helixc::parser::helix_parser::HelixParser::parse_source(&query).unwrap();
        


        Ok("".to_string())
    }
}

#[derive(Debug)]
pub enum HelixLiteError {
    Default(String),
}

impl std::fmt::Display for HelixLiteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HelixLiteError::Default(msg) => write!(f, "Graph error: {}", msg)
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

