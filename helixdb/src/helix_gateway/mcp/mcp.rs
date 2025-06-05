// provides tool endpoints for mcp
// init endpoint to get a user id and establish a connection to helix server

// wraps iter in new tools

use std::{collections::HashMap, sync::Arc, vec::IntoIter};

use heed3::{AnyTls, RoTxn};

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
    protocol::label_hash::hash_label,
};

pub(crate) struct McpBackend<'a> {
        pub connections: HashMap<&'a str, MCPConnection<'a>>,
    pub db: Arc<HelixGraphStorage>,
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
