// provides tool endpoints for mcp
// init endpoint to get a user id and establish a connection to helix server

// wraps iter in new tools

use crate::helix_engine::{
    graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
    types::GraphError,
};

pub struct MCPConnection<'a, I> {
    pub connection_id: &'a str,
    pub connection_addr: &'a str,
    pub connection_port: u16,
    pub iter: McpIter<I>,
}

pub struct McpIter<I> {

    pub iter: I,
}

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
