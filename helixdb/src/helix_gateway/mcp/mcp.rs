// provides tool endpoints for mcp
// init endpoint to get a user id and establish a connection to helix server

// wraps iter in new tools

use std::{collections::HashMap, sync::Arc};

use crate::{helix_engine::{
    graph_core::{
        graph_core::HelixGraphEngine,
        ops::{out::out::OutNodesIterator, source::add_e::EdgeType, tr_val::{Traversable, TraversalVal}},
        traversal_iter::RoTraversalIterator,
    },
    storage_core::storage_core::HelixGraphStorage,
    types::GraphError,
}, protocol::label_hash::hash_label};

pub(crate) struct McpBackend<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    pub connections: HashMap<&'a str, MCPConnection<'a, I>>,
    pub db: Arc<HelixGraphStorage>,
}

impl<'a, I> McpBackend<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
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

    pub fn add_connection(&'a mut self, connection: MCPConnection<'a, I>) {
        self.connections
            .insert(connection.connection_id, connection);
    }

    pub fn remove_connection(&mut self, connection_id: &'a str) {
        self.connections.remove(connection_id);
    }

    pub fn get_connection(&'a self, connection_id: &'a str) -> Option<&'a MCPConnection<'a, I>> {
        self.connections.get(connection_id)
    }

    pub fn get_connection_mut(
        &'a mut self,
        connection_id: &'a str,
    ) -> Option<&'a mut MCPConnection<'a, I>> {
        self.connections.get_mut(connection_id)
    }

    pub fn out_step(
        &'a mut self,
        connection_id: &'a str,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
    ) -> Result<TraversalVal, GraphError> {
        let connection = self.get_connection_mut(connection_id).unwrap();
        let txn = self.db.graph_env.read_txn()?;

        let iter = connection
            .iter
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key(&item.unwrap().id(), &edge_label_hash);
                match self.db
                    .out_edges_db
                    .lazily_decode_data()
                    .get_duplicates(&txn, &prefix)
                {
                    Ok(Some(iter)) => Some(OutNodesIterator {
                        iter,
                        storage: Arc::clone(&self.db),
                        edge_type,
                        txn: &txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        match edge_type {
            EdgeType::Node => {}
            EdgeType::Vec => {}
        }
        connection.iter.out_step(edge_label, edge_type)
    }
}

pub(crate) struct MCPConnection<'a, I> {
    pub connection_id: &'a str,
    pub connection_addr: &'a str,
    pub connection_port: u16,
    pub iter: I,
}

// pub struct McpIter<I> {
//     pub iter: I,
// }

impl<'a, I> MCPConnection<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    pub fn new(
        connection_id: &'a str,
        connection_addr: &'a str,
        connection_port: u16,
        iter: I,
    ) -> Self {
        Self {
            connection_id,
            connection_addr,
            connection_port,
            iter,
        }
    }
}
