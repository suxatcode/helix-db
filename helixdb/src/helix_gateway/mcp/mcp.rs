// provides tool endpoints for mcp
// init endpoint to get a user id and establish a connection to helix server

// wraps iter in new tools

use std::{collections::HashMap, sync::Arc, vec::IntoIter};

use get_routes::{local_handler, mcp_handler};
use heed3::{AnyTls, RoTxn};
use serde::Deserialize;

use crate::{
    helix_engine::{
        graph_core::{
            graph_core::HelixGraphEngine,
            ops::{
                in_::{in_::InNodesIterator, in_e::InEdgesIterator},
                out::{out::OutNodesIterator, out_e::OutEdgesIterator},
                source::{add_e::EdgeType, n_from_type::NFromType},
                tr_val::{Traversable, TraversalVal},
            },
            traversal_iter::RoTraversalIterator,
        },
        storage_core::storage_core::HelixGraphStorage,
        types::GraphError,
    },
    helix_gateway::{mcp::tools::ToolArgs, router::router::HandlerInput},
    protocol::{label_hash::hash_label, request::Request, response::Response},
};

pub(crate) struct McpBackend<'a> {
    pub connections: HashMap<&'a str, MCPConnection<'a>>,
    pub db: Arc<HelixGraphStorage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolCallRequest {
    pub connection_id: String,
    pub tool: ToolArgs,
}

impl<'a> McpBackend<'a> {
    pub fn new(db: Arc<HelixGraphStorage>) -> Self {
        Self {
            connections: HashMap::new(),
            db,
        }
    }
    pub fn new_with_max_connections(db: Arc<HelixGraphStorage>, max_connections: usize) -> Self {
        Self {
            connections: HashMap::with_capacity(max_connections),
            db,
        }
    }

    pub fn add_connection(&'a mut self, connection: MCPConnection<'a>) {
        self.connections
            .insert(connection.connection_id, connection);
    }

    pub fn remove_connection(&mut self, connection_id: &'a str) -> Option<MCPConnection<'a>> {
        self.connections.remove(connection_id)
    }

    pub fn get_connection(&'a self, connection_id: &'a str) -> Option<&'a MCPConnection<'a>> {
        self.connections.get(connection_id)
    }

    pub fn get_connection_mut(
        &'a mut self,
        connection_id: &'a str,
    ) -> Option<&'a mut MCPConnection<'a>> {
        self.connections.get_mut(connection_id)
    }
}

pub(crate) struct MCPConnection<'a> {
    pub connection_id: &'a str,
    pub connection_addr: &'a str,
    pub connection_port: u16,
    pub iter: IntoIter<TraversalVal>,
}

// pub struct McpIter<I> {
//     pub iter: I,
// }

impl<'a> MCPConnection<'a> {
    pub fn new(
        connection_id: &'a str,
        connection_addr: &'a str,
        connection_port: u16,
        iter: IntoIter<TraversalVal>,
    ) -> Self {
        Self {
            connection_id,
            connection_addr,
            connection_port,
            iter,
        }
    }
}

pub struct MCPToolInput {
    pub request: Request,
    pub graph: Arc<HelixGraphEngine>,
}

// basic type for function pointer
pub type BasicMCPHandlerFn = fn(&MCPToolInput, &mut Response) -> Result<(), GraphError>;

// thread safe type for multi threaded use
pub type MCPHandlerFn =
    Arc<dyn Fn(&MCPToolInput, &mut Response) -> Result<(), GraphError> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct MCPHandlerSubmission(pub MCPHandler);

#[derive(Clone, Debug)]
pub struct MCPHandler {
    pub name: &'static str,
    pub func: BasicMCPHandlerFn,
}

impl MCPHandler {
    pub const fn new(name: &'static str, func: BasicMCPHandlerFn) -> Self {
        Self { name, func }
    }
}

inventory::collect!(MCPHandlerSubmission);

#[mcp_handler]
pub fn call_tool(input: &MCPToolInput, response: &mut Response) -> Result<(), GraphError> {
    let data: ToolCallRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };
    Ok(())
}
