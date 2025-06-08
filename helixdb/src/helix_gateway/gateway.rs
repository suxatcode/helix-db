use std::{collections::HashMap, sync::Arc};

use super::connection::connection::ConnectionHandler;
use crate::helix_runtime::AsyncRuntime;
use super::router::router::{HandlerFn, HelixRouter};
use crate::{
    helix_engine::graph_core::graph_core::HelixGraphEngine, helix_gateway::mcp::mcp::MCPHandlerFn,
};

pub struct GatewayOpts {}

impl GatewayOpts {
    pub const DEFAULT_POOL_SIZE: usize = 8;
}

pub struct HelixGateway<R: AsyncRuntime + Clone + Send + Sync + 'static> {
    pub connection_handler: ConnectionHandler<R>,
    pub runtime: R,
}

impl<R: AsyncRuntime + Clone + Send + Sync + 'static> HelixGateway<R> {
    pub async fn new(
        address: &str,
        graph: Arc<HelixGraphEngine>,
        size: usize,
        routes: Option<HashMap<(String, String), HandlerFn>>,
        mcp_routes: Option<HashMap<(String, String), MCPHandlerFn>>,
        runtime: R,
    ) -> HelixGateway<R> {
        let router = HelixRouter::new(routes, mcp_routes);
        let connection_handler =
            ConnectionHandler::new(address, graph, size, router, runtime.clone()).unwrap();
        println!("Gateway created");
        HelixGateway {
            connection_handler,
            runtime,
        }
    }
}
