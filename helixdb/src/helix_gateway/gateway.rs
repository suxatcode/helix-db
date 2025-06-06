use std::{collections::HashMap, sync::Arc};

use super::connection::connection::ConnectionHandler;
use super::router::router::{HandlerFn, HelixRouter};
use crate::{
    helix_engine::graph_core::graph_core::HelixGraphEngine, helix_gateway::mcp::mcp::MCPHandlerFn,
};

pub struct GatewayOpts {}

impl GatewayOpts {
    pub const DEFAULT_POOL_SIZE: usize = 8;
}

pub struct HelixGateway {
    pub connection_handler: ConnectionHandler,
}

impl HelixGateway {
    pub async fn new(
        address: &str,
        graph: Arc<HelixGraphEngine>,
        size: usize,
        routes: Option<HashMap<(String, String), HandlerFn>>,
        mcp_routes: Option<HashMap<(String, String), MCPHandlerFn>>,
    ) -> HelixGateway {
        let router = HelixRouter::new(routes, mcp_routes);
        let connection_handler = ConnectionHandler::new(address, graph, size, router).unwrap();
        println!("Gateway created");
        HelixGateway { connection_handler }
    }
}
