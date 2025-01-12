use protocol::{traversal_value::TraversalValue, count::Count, Value};

use crate::types::GraphError;

pub trait SourceTraversalSteps {
    /// Adds all nodes in the graph to current traversal step
    ///
    /// Note: This can be a VERY expensive operation
    fn v(&mut self) -> &mut Self;
    /// Adds all edges in the graph to current traversal step
    ///  
    /// Note: This can be a VERY expensive operation
    fn e(&mut self) -> &mut Self;

    /// Creates a new node in the graph and adds it to current traversal step
    fn add_v(&mut self, node_label: &str, props: Vec<(String, Value)>) -> &mut Self;
    /// Creates a new edge in the graph between two nodes and adds it to current traversal step
    fn add_e(
        &mut self,
        edge_label: &str,
        from_id: &str,
        to_id: &str,
        props: Vec<(String, Value)>,
    ) -> &mut Self;

    /// Adds node with specific id to current traversal step
    fn v_from_id(&mut self, node_id: &str) -> &mut Self;

    /// Adds edge with specific id to current traversal step
    fn e_from_id(&mut self, edge_id: &str) -> &mut Self;
}

pub trait TraversalSteps {
    /// Adds the nodes at the end of an outgoing edge to the current traversal step that match a given edge label if given one
    fn out(&mut self, edge_label: &str) -> &mut Self;
    /// Adds the outgoing edges from the current node to the current traversal step that match a given edge label if given one
    fn out_e(&mut self, edge_label: &str) -> &mut Self;

    /// Adds the nodes at the start of an incoming edge to the current traversal step that match a given edge label if given one
    fn in_(&mut self, edge_label: &str) -> &mut Self;
    /// Adds the incoming edges from the current node
    /// to the current traversal step that match a given edge label if given one
    fn in_e(&mut self, edge_label: &str) -> &mut Self;

    /// Adds the node that recieves the current edge to the current traversal step
    fn in_v(&mut self) -> &mut Self;

    /// Adds the node that sends the current edge to the current traversal step
    /// to the current traversal step
    fn out_v(&mut self) -> &mut Self;

    /// Adds the nodes at the ends of both the incoming and outgoing edges from the current node to the current traversal step
    /// that match a given edge label if given one
    fn both(&mut self, edge_label: &str) -> &mut Self;

    /// Adds both the incoming and outgoing edges from the current node to the current traversal step
    /// that match a given edge label if given one
    fn both_e(&mut self, edge_label: &str) -> &mut Self;

    /// Adds the nodes at the ends of both the incoming and outgoing edges from the current node to the current traversal step
    fn both_v(&mut self) -> &mut Self;
}

pub trait TraversalMethods {
    /// Flattens everything in the current traversal step and counts how many items there are.
    fn count(&mut self) -> &mut Self;

    /// Flattens everything in the current traversal step and updates the current traversal step to be a slice of itself.
    fn range(&mut self, start: usize, end: usize) -> &mut Self;

    /// Filters the current traversal step
    ///
    /// ### Returns:
    /// - The traversal builder with the current step overwritten with the remaining values
    ///
    /// ## Example
    /// ```rust
    ///
    /// use helix_engine::graph_core::traversal::TraversalBuilder;
    /// use helix_engine::graph_core::traversal_value::TraversalValue;
    /// use helix_engine::graph_core::graph_core::HelixGraphEngine;
    /// use helix_engine::storage_core::storage_core::HelixGraphStorage;
    /// use helix_engine::storage_core::storage_methods::StorageMethods;
    /// use helix_engine::graph_core::traversal_steps::*;
    /// use helix_engine::props;
    /// use protocol::Value;
    /// use tempfile::TempDir;
    ///
    /// let temp_dir = TempDir::new().unwrap();
    /// let db_path = temp_dir.path().to_str().unwrap();
    /// let engine = HelixGraphEngine::new(db_path).unwrap();
    ///
    /// let _ = engine.storage
    ///     .create_node("person", props! { "age" => 25, "name" => "Alice" })
    ///     .unwrap();
    /// let person2 = engine.storage
    ///     .create_node("person", props! { "age" => 30, "name" => "Bob" })
    ///     .unwrap();
    /// let _ = engine.storage
    ///     .create_node("person", props! { "age" => 35 })
    ///     .unwrap();
    ///
    /// let mut traversal = TraversalBuilder::new(&engine.storage, vec![]);
    ///
    /// fn age_greater_than(val: &TraversalValue, min_age: i32) -> bool {
    ///     if let TraversalValue::SingleNode(node) = val {
    ///         if let Some(Value::Integer(age)) = node.properties.get("age") {
    ///             return *age > min_age;
    ///         }
    ///     }
    ///     false
    /// }
    ///
    /// fn has_name(val: &TraversalValue) -> bool {
    ///     if let TraversalValue::SingleNode(node) = val {
    ///         return node.properties.contains_key("name");
    ///     }
    ///     false
    /// }
    ///
    /// // Example With Closure
    /// let test_with_closure = traversal.v().filter(|val| {
    ///     if let TraversalValue::SingleNode(node) = val {
    ///         if let Some(Value::Integer(age)) = node.properties.get("age") {
    ///             return *age > 25;
    ///         }
    ///     }
    ///     false
    /// }).count();
    ///    
    /// // Example passing function that takes input
    /// let test_calling_function_with_inputs = traversal.v().filter(|node| age_greater_than(node, 30)).count();
    ///  
    /// // Example passing function that takes NO input
    /// let test_calling_function_without_inputs = traversal.v().filter(has_name).count();
    ///
    /// // Example of chained traversal
    /// let test_chained_traversal = traversal
    ///     .filter(has_name)
    ///     .filter(|val| age_greater_than(val, 27)).count();
    ///
    /// assert_eq!(test_with_closure, 2);
    /// assert_eq!(test_calling_function_with_inputs, 1);
    /// assert_eq!(test_calling_function_without_inputs, 2);
    /// assert_eq!(test_chained_traversal, 1);
    /// ```
    fn filter<F>(&mut self, predicate: F) -> &mut Self
    where
        F: Fn(&TraversalValue) -> Result<bool, GraphError>;

    /// Maps the current traversal step to a new traversal step
    fn get_properties(&mut self, keys: &Vec<String>) -> &mut Self;
}
