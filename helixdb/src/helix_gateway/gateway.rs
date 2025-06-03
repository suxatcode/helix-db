use std::{collections::HashMap, sync::Arc};

use super::connection::connection::ConnectionHandler;
use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use super::router::router::{HandlerFn, HelixRouter};

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
    ) -> HelixGateway {
        let router = HelixRouter::new(routes);
        let connection_handler = ConnectionHandler::new(address, graph, size, router).unwrap();
        println!("Gateway created");
        HelixGateway { connection_handler }
    }
}

