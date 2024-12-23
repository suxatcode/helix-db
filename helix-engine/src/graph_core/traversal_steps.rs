use crate::{storage_core::storage_core::HelixGraphStorage, types::GraphError};

pub trait SourceTraversalSteps {
    /// Fetches all nodes in the graph 
    /// 
    /// Note: This can be a VERY expensive operation
    fn v(&mut self, storage: &HelixGraphStorage) -> &mut Self;
     /// Fetches all edges in the graph 
    /// 
    /// Note: This can be a VERY expensive operation
    fn e(&mut self, storage: &HelixGraphStorage) -> &mut Self;

    /// Adds a node to the graph
    fn add_v(&mut self, storage: &HelixGraphStorage, node_label: &str) -> &mut Self;
    /// Adds an edge to the graph between two nodes
    fn add_e(&mut self, storage: &HelixGraphStorage, edge_label: &str, from_id: &str, to_id: &str) -> &mut Self;
}

pub trait TraversalSteps {
    /// Adds the nodes at the end of an outgoing edge with a given edge label from the current node
    fn out(&mut self, storage: &HelixGraphStorage, edge_label: &str) -> &mut Self;
    /// Adds the outgoing edges from the current node that match a given edge label
    fn out_e(&mut self, storage: &HelixGraphStorage, edge_label: &str) -> &mut Self;
    
    /// Adds the nodes at the start of an incoming edge with a given edge label to the current node
    fn in_(&mut self, storage: &HelixGraphStorage, edge_label: &str) -> &mut Self;
    /// Adds the incoming edges from the current node that match a given edge label
    fn in_e(&mut self, storage: &HelixGraphStorage, edge_label: &str) -> &mut Self;

}

pub trait TraversalMethods {
    fn count(&mut self) -> usize;

    fn range(&mut self, start: usize, end: usize) -> &mut Self;
}