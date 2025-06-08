// router

// takes in raw [u8] data
// parses to request type

// then locks graph and passes parsed data and graph to handler to execute query

// returns response

use crate::{
    helix_engine::{graph_core::graph_core::HelixGraphEngine, types::GraphError},
    helix_gateway::mcp::mcp::{MCPHandlerFn, MCPToolInput, McpConnections},
    helix_storage::lmdb_storage::LmdbStorage,
};
use core::fmt;
use std::{collections::HashMap, sync::Arc};

use crate::protocol::{request::Request, response::Response};

pub struct HandlerInput {
    pub request: Request,
    pub graph: Arc<HelixGraphEngine<LmdbStorage>>,
}

// basic type for function pointer
pub type BasicHandlerFn = fn(&HandlerInput, &mut Response) -> Result<(), GraphError>;

// thread safe type for multi threaded use
pub type HandlerFn =
    Arc<dyn Fn(&HandlerInput, &mut Response) -> Result<(), GraphError> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct HandlerSubmission(pub Handler);

#[derive(Clone, Debug)]
pub struct Handler {
    pub name: &'static str,
    pub func: BasicHandlerFn,
}

impl Handler {
    pub const fn new(name: &'static str, func: BasicHandlerFn) -> Self {
        Self { name, func }
    }
}

inventory::collect!(HandlerSubmission);

pub struct HelixRouter {
    /// Method+Path => Function
    pub routes: HashMap<(String, String), HandlerFn>,
    pub mcp_routes: HashMap<(String, String), MCPHandlerFn>,
}

impl HelixRouter {
    /// Create a new router with a set of routes
    pub fn new(
        routes: Option<HashMap<(String, String), HandlerFn>>,
        mcp_routes: Option<HashMap<(String, String), MCPHandlerFn>>,
    ) -> Self {
        let rts = routes.unwrap_or_default();
        let mcp_rts = mcp_routes.unwrap_or_default();
        Self {
            routes: rts,
            mcp_routes: mcp_rts,
        }
    }

    /// Add a route to the router
    pub fn add_route(&mut self, method: &str, path: &str, handler: BasicHandlerFn) {
        self.routes
            .insert((method.to_uppercase(), path.to_string()), Arc::new(handler));
    }

    /// Handle a request by finding the appropriate handler and executing it
    ///
    /// ## Arguments
    ///
    /// * `graph_access` - A reference to the graph engine
    /// * `request` - The request to handle
    /// * `response` - The response to write to
    ///
    /// ## Returns
    ///
    /// * `Ok(())` if the request was handled successfully
    /// * `Err(RouterError)` if there was an error handling the request
    pub fn handle(
        &self,
        graph_access: Arc<HelixGraphEngine<LmdbStorage>>,
        request: Request,
        response: &mut Response,
    ) -> Result<(), GraphError> {
        let route_key = (request.method.clone(), request.path.clone());

        if let Some(handler) = self.routes.get(&route_key) {
            let input = HandlerInput {
                request,
                graph: Arc::clone(&graph_access),
            };
            return handler(&input, response);
        }

        if let Some(mcp_handler) = self.mcp_routes.get(&route_key) {
            let mut mcp_input = MCPToolInput {
                request,
                mcp_backend: Arc::clone(graph_access.mcp_backend.as_ref().unwrap()),
                mcp_connections: Arc::clone(graph_access.mcp_connections.as_ref().unwrap()),
            };
            return mcp_handler(&mut mcp_input, response);
        };

        response.status = 404;
        response.body = b"404 - Not Found".to_vec();
        Ok(())
    }
}

#[derive(Debug)]
pub enum RouterError {
    Io(std::io::Error),
    New(String),
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RouterError::Io(e) => write!(f, "IO error: {}", e),
            RouterError::New(msg) => write!(f, "Graph error: {}", msg),
        }
    }
}

impl From<String> for RouterError {
    fn from(error: String) -> Self {
        RouterError::New(error)
    }
}

impl From<std::io::Error> for RouterError {
    fn from(error: std::io::Error) -> Self {
        RouterError::Io(error)
    }
}

impl From<GraphError> for RouterError {
    fn from(error: GraphError) -> Self {
        RouterError::New(error.to_string())
    }
}

impl From<RouterError> for GraphError {
    fn from(error: RouterError) -> Self {
        GraphError::New(error.to_string())
    }
}
