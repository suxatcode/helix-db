use protocol::Node;

use crate::{storage_core::storage_core::HelixGraphStorage, types::GraphError};

use super::{count::Count, traversal_value::{AsTraversalValue, TraversalValue}};

pub trait SourceTraversalSteps {
    /// Adds all nodes in the graph to current traversal step
    ///
    /// Note: This can be a VERY expensive operation
    fn v(&mut self, storage: &HelixGraphStorage) -> &mut Self;
    /// Adds all edges in the graph to current traversal step
    ///  
    /// Note: This can be a VERY expensive operation
    fn e(&mut self, storage: &HelixGraphStorage) -> &mut Self;

    /// Creates a new node in the graph and adds it to current traversal step
    fn add_v(&mut self, storage: &HelixGraphStorage, node_label: &str) -> &mut Self;
    /// Creates a new edge in the graph between two nodes and adds it to current traversal step
    fn add_e(
        &mut self,
        storage: &HelixGraphStorage,
        edge_label: &str,
        from_id: &str,
        to_id: &str,
    ) -> &mut Self;

    /// Adds node with specific id to current traversal step
    fn v_from_id(&mut self, storage: &HelixGraphStorage, node_id: &str) -> &mut Self;

    /// Adds edge with specific id to current traversal step
    fn e_from_id(&mut self, storage: &HelixGraphStorage, edge_id: &str) -> &mut Self;
}

pub trait TraversalSteps {
    /// Adds the nodes at the end of an outgoing edge with a given edge label
    /// from the current node to the current traversal step
    fn out(&mut self, storage: &HelixGraphStorage, edge_label: &str) -> &mut Self;
    /// Adds the outgoing edges from the current node that match a given edge label
    /// to the current traversal step
    fn out_e(&mut self, storage: &HelixGraphStorage, edge_label: &str) -> &mut Self;

    /// Adds the nodes at the start of an incoming edge with a given edge label
    /// to the current node to the current traversal step
    fn in_(&mut self, storage: &HelixGraphStorage, edge_label: &str) -> &mut Self;
    /// Adds the incoming edges from the current node that match a given edge label
    /// to the current traversal step
    fn in_e(&mut self, storage: &HelixGraphStorage, edge_label: &str) -> &mut Self;
}

pub trait TraversalMethods {
    /// Flattens everything in the current traversal step and counts how many items there are.
    fn count(&mut self) -> Count;

    /// Flattens everything in the current traversal step and updates the current traversal step to be a slice of itself.
    fn range(&mut self, start: usize, end: usize) -> &mut Self;

    fn filter<F>(&mut self, predicate: F) -> &mut Self
    where
        F: Fn(&TraversalValue) -> bool;
}