use crate::{
    graph_core::traversal_steps::{SourceTraversalSteps, TraversalMethods, TraversalSteps},
    props,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use core::panic;
use protocol::{count::Count, traversal_value::TraversalValue, Edge, Filterable, Node, Value};
use std::collections::HashMap;

pub struct TraversalBuilder<'a> {
    pub variables: HashMap<String, TraversalValue>,
    pub current_step: TraversalValue,
    pub storage: &'a HelixGraphStorage,
    pub error: Option<GraphError>,
}

impl<'a> TraversalBuilder<'a> {
    pub fn new(storage: &'a HelixGraphStorage, start_nodes: TraversalValue) -> Self {
        Self {
            variables: HashMap::from_iter(std::iter::empty()),
            current_step: start_nodes,
            storage,
            error: None,
        }
    }

    pub fn check_is_valid_node_traversal(&self, function_name: &str) -> Result<(), GraphError> {
        match matches!(self.current_step, TraversalValue::NodeArray(_)) {
            true => Ok(()),
            false => Err(GraphError::TraversalError(format!(
                "The traversal step {:?}, is not a valid traversal from an edge. 
                The current step should be a node.",
                function_name
            ))),
        }
    }

    pub fn check_is_valid_edge_traversal(&self, function_name: &str) -> Result<(), GraphError> {
        match matches!(self.current_step, TraversalValue::EdgeArray(_)) {
            true => Ok(()),
            false => Err(GraphError::TraversalError(format!(
                "The traversal step {:?}, is not a valid traversal from a node. 
                The current step should be an edge",
                function_name
            ))),
        }
    }

    fn store_error(&mut self, err: GraphError) -> &mut Self {
        if self.error.is_none() {
            self.error = Some(err);
        }
        self
    }
}

impl<'a> SourceTraversalSteps for TraversalBuilder<'a> {
    fn v(&mut self) -> &mut Self {
        match self.storage.get_all_nodes() {
            Ok(nodes) => {
                self.current_step = TraversalValue::NodeArray(nodes);
            }
            Err(err) => {
                self.store_error(err);
            }
        }
        self
    }

    fn e(&mut self) -> &mut Self {
        match self.storage.get_all_edges() {
            Ok(edges) => {
                self.current_step = TraversalValue::EdgeArray(edges);
            }
            Err(err) => {
                self.store_error(err);
            }
        }
        self
    }

    fn add_v(&mut self, node_label: &str, props: Vec<(String, Value)>) -> &mut Self {
        match self.storage.create_node(node_label, props) {
            Ok(node) => {
                self.current_step = TraversalValue::from(node);
            }
            Err(err) => {
                self.store_error(err);
            }
        }
        self
    }

    fn add_e(
        &mut self,
        edge_label: &str,
        from_id: &str,
        to_id: &str,
        props: Vec<(String, Value)>,
    ) -> &mut Self {
        match self.storage.create_edge(edge_label, from_id, to_id, props) {
            Ok(edge) => {
                self.current_step = TraversalValue::from(edge);
            }
            Err(err) => {
                self.store_error(err);
            }
        }
        self
    }

    fn v_from_id(&mut self, node_id: &str) -> &mut Self {
        match self.storage.get_node(node_id) {
            Ok(node) => {
                self.current_step = TraversalValue::from(node);
            }
            Err(err) => {
                self.store_error(err);
            }
        }
        self
    }

    fn e_from_id(&mut self, edge_id: &str) -> &mut Self {
        match self.storage.get_edge(edge_id) {
            Ok(edge) => {
                self.current_step = TraversalValue::from(edge);
            }
            Err(err) => {
                self.store_error(err);
            }
        }
        self
    }
}

impl<'a> TraversalSteps for TraversalBuilder<'a> {
    fn out(&mut self, edge_label: &str) -> &mut Self {
        let mut e = GraphError::Default;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self.storage.get_out_nodes(&node.id, edge_label) {
                    Ok(nodes) => match nodes.is_empty() {
                        false => new_current.extend(nodes),
                        true => continue,
                    },
                    Err(err) => e = err,
                }
            }
            if new_current.is_empty() {
                self.current_step = TraversalValue::Empty;
            } else {
                self.current_step = TraversalValue::NodeArray(new_current);
            }
        }
        self.store_error(e);
        self
    }

    fn out_e(&mut self, edge_label: &str) -> &mut Self {
        let mut e = GraphError::Default;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self.storage.get_out_edges(&node.id, edge_label) {
                    Ok(edges) => match edges.is_empty() {
                        false => new_current.extend(edges),
                        true => continue,
                    },
                    Err(err) => e = err,
                }
            }
            if new_current.is_empty() {
                self.current_step = TraversalValue::Empty;
            } else {
                self.current_step = TraversalValue::EdgeArray(new_current);
            }
        }
        self.store_error(e);
        self
    }

    fn in_(&mut self, edge_label: &str) -> &mut Self {
        let mut e = GraphError::Default;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self.storage.get_in_nodes(&node.id, edge_label) {
                    Ok(nodes) => match nodes.is_empty() {
                        false => new_current.extend(nodes),
                        true => continue,
                    },
                    Err(err) => e = err,
                }
            }
            if new_current.is_empty() {
                self.current_step = TraversalValue::Empty;
            } else {
                self.current_step = TraversalValue::NodeArray(new_current);
            }
        }
        self.store_error(e);
        self
    }

    fn in_e(&mut self, edge_label: &str) -> &mut Self {
        let mut e = GraphError::Default;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self.storage.get_in_edges(&node.id, edge_label) {
                    Ok(edges) => match edges.is_empty() {
                        false => new_current.extend(edges),
                        true => continue,
                    },
                    Err(err) => e = err,
                }
            }
            if new_current.is_empty() {
                self.current_step = TraversalValue::Empty;
            } else {
                self.current_step = TraversalValue::EdgeArray(new_current);
            }
        }
        self.store_error(e);
        self
    }

    fn both_e(&mut self, edge_label: &str) -> &mut Self {
        let mut e = GraphError::Default;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self.storage.get_in_edges(&node.id, edge_label) {
                    Ok(in_edges) => {
                        if !in_edges.is_empty() {
                            new_current.extend(in_edges);
                        }
                    }
                    Err(err) => e = err,
                }
                match self.storage.get_out_edges(&node.id, edge_label) {
                    Ok(out_edges) => {
                        if !out_edges.is_empty() {
                            new_current.extend(out_edges);
                        }
                    }
                    Err(err) => e = err,
                }
            }
            if new_current.is_empty() {
                self.current_step = TraversalValue::Empty;
            } else {
                self.current_step = TraversalValue::EdgeArray(new_current);
            }
        }
        self.store_error(e);
        self
    }

    fn both(&mut self, edge_label: &str) -> &mut Self {
        let mut e = GraphError::Default;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self.storage.get_in_nodes(&node.id, edge_label) {
                    Ok(in_nodes) => {
                        if !in_nodes.is_empty() {
                            new_current.extend(in_nodes);
                        }
                    }
                    Err(err) => e = err,
                }
                match self.storage.get_out_nodes(&node.id, edge_label) {
                    Ok(out_nodes) => {
                        if !out_nodes.is_empty() {
                            new_current.extend(out_nodes);
                        }
                    }
                    Err(err) => e = err,
                }
            }
            if new_current.is_empty() {
                self.current_step = TraversalValue::Empty;
            } else {
                self.current_step = TraversalValue::NodeArray(new_current);
            }
        }
        self.store_error(e);
        self
    }

    fn out_v(&mut self) -> &mut Self {
        let mut e = GraphError::Default;
        if let TraversalValue::EdgeArray(edges) = &self.current_step {
            let mut new_current = Vec::with_capacity(edges.len());
            for edge in edges {
                match self.storage.get_node(&edge.from_node) {
                    Ok(node) => new_current.push(node),
                    Err(err) => e = err,
                }
            }
            if new_current.is_empty() {
                self.current_step = TraversalValue::Empty;
            } else {
                self.current_step = TraversalValue::NodeArray(new_current);
            }
        }
        self.store_error(e);
        self
    }

    fn in_v(&mut self) -> &mut Self {
        let mut e = GraphError::Default;
        if let TraversalValue::EdgeArray(edges) = &self.current_step {
            let mut new_current = Vec::with_capacity(edges.len());
            for edge in edges {
                match self.storage.get_node(&edge.to_node) {
                    Ok(node) => new_current.push(node),
                    Err(err) => e = err,
                }
            }
            if new_current.is_empty() {
                self.current_step = TraversalValue::Empty;
            } else {
                self.current_step = TraversalValue::NodeArray(new_current);
            }
        }
        self.store_error(e);
        self
    }

    fn both_v(&mut self) -> &mut Self {
        let mut e = GraphError::Default;
        if let TraversalValue::EdgeArray(edges) = &self.current_step {
            let mut new_current = Vec::with_capacity(edges.len() * 2);
            for edge in edges {
                match self.storage.get_node(&edge.from_node) {
                    Ok(node) => new_current.push(node),
                    Err(err) => e = err,
                }
                match self.storage.get_node(&edge.to_node) {
                    Ok(node) => new_current.push(node),
                    Err(err) => e = err,
                }
            }
            if new_current.is_empty() {
                self.current_step = TraversalValue::Empty;
            } else {
                self.current_step = TraversalValue::NodeArray(new_current);
            }
        }
        self.store_error(e);
        self
    }
}

impl<'a> TraversalMethods for TraversalBuilder<'a> {
    fn count(&mut self) -> &mut Self {
        self.current_step = TraversalValue::Count(Count::new(match &self.current_step {
            TraversalValue::NodeArray(nodes) => nodes.len(),
            TraversalValue::EdgeArray(edges) => edges.len(),
            TraversalValue::Empty => 0,
            _ => panic!("Invalid traversal step for count"),
        }));
        self
    }
    fn range(&mut self, start: usize, end: usize) -> &mut Self {
        match &self.current_step {
            TraversalValue::NodeArray(nodes) => {
                let new_current = nodes[start..end].to_vec();
                self.current_step = TraversalValue::NodeArray(new_current);
            }
            TraversalValue::EdgeArray(edges) => {
                let new_current = edges[start..end].to_vec();
                self.current_step = TraversalValue::EdgeArray(new_current);
            }
            _ => panic!("Invalid traversal step for range"),
        }
        self
    }

    // Then modify the filter function
    fn filter_nodes<F>(&mut self, predicate: F) -> &mut Self
    where
        F: Fn(&Node) -> Result<bool, GraphError>,
    {
        if let TraversalValue::NodeArray(nodes) = &mut self.current_step {
            nodes.retain(|node| predicate(node).unwrap());
        }
        self
    }

    fn filter_edges<F>(&mut self, predicate: F) -> &mut Self
    where
        F: Fn(&Edge) -> Result<bool, GraphError>,
    {
        if let TraversalValue::EdgeArray(edges) = &mut self.current_step {
            edges.retain(|edge| predicate(edge).unwrap());
        }
        self
    }

    fn get_properties(&mut self, keys: &Vec<String>) -> &mut Self {
        match &mut self.current_step {
            TraversalValue::NodeArray(nodes) => {
                let mut new_props = Vec::with_capacity(nodes.len() * keys.len());
                for node in nodes {
                    let vals = keys
                        .iter()
                        .map(|key| {
                            if let Some(value) = node.check_property(key) {
                                (key.clone(), value.clone())
                            } else {
                                (key.clone(), Value::Empty)
                            }
                        })
                        .collect::<Vec<_>>();
                    new_props.extend(vals);
                }
                self.current_step = TraversalValue::ValueArray(new_props);
            }
            TraversalValue::EdgeArray(edges) => {
                let mut new_props = Vec::with_capacity(edges.len() * keys.len());
                for edge in edges {
                    let vals = keys
                        .iter()
                        .map(|key| {
                            if let Some(value) = edge.check_property(key) {
                                (key.clone(), value.clone())
                            } else {
                                (key.clone(), Value::Empty)
                            }
                        })
                        .collect::<Vec<_>>();
                    new_props.extend(vals);
                }
                self.current_step = TraversalValue::ValueArray(new_props);
            }
            _ => panic!("Invalid traversal step for get_properties"),
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{TraversalBuilder, TraversalMethods, TraversalSteps};
    use crate::{
        graph_core::traversal_steps::SourceTraversalSteps,
        props,
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    };
    use protocol::{traversal_value::TraversalValue, Edge, Filterable, Node, Value};
    use rayon::vec;
    use tempfile::TempDir;

    fn setup_test_db() -> (HelixGraphStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();
        let storage = HelixGraphStorage::new(db_path).unwrap();
        (storage, temp_dir)
    }

    #[test]
    fn test_v() {
        let (storage, _temp_dir) = setup_test_db();

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        let thing = storage.create_node("thing", props!()).unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v();

        // Check that the node array contains all nodes
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 3);

                let node_ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
                let node_labels: Vec<String> = nodes.iter().map(|n| n.label.clone()).collect();

                assert!(node_ids.contains(&person1.id));
                assert!(node_ids.contains(&person2.id));
                assert!(node_ids.contains(&thing.id));

                assert_eq!(node_labels.iter().filter(|&l| l == "person").count(), 2);
                assert_eq!(node_labels.iter().filter(|&l| l == "thing").count(), 1);
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    #[test]
    fn test_e() {
        let (storage, _temp_dir) = setup_test_db();

        // Graph Structure:
        // (person1)-[knows]->(person2)
        //         \-[likes]->(person3)
        // (person2)-[follows]->(person3)

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        let person3 = storage.create_node("person", props!()).unwrap();

        let knows_edge = storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();
        let likes_edge = storage
            .create_edge("likes", &person1.id, &person3.id, props!())
            .unwrap();
        let follows_edge = storage
            .create_edge("follows", &person2.id, &person3.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.e();

        // Check that the edge array contains the three edges
        match &traversal.current_step {
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 3);

                let edge_ids: Vec<String> = edges.iter().map(|e| e.id.clone()).collect();
                let edge_labels: Vec<String> = edges.iter().map(|e| e.label.clone()).collect();

                assert!(edge_ids.contains(&knows_edge.id));
                assert!(edge_ids.contains(&likes_edge.id));
                assert!(edge_ids.contains(&follows_edge.id));

                assert!(edge_labels.contains(&"knows".to_string()));
                assert!(edge_labels.contains(&"likes".to_string()));
                assert!(edge_labels.contains(&"follows".to_string()));

                for edge in edges {
                    match edge.label.as_str() {
                        "knows" => {
                            assert_eq!(edge.from_node, person1.id);
                            assert_eq!(edge.to_node, person2.id);
                        }
                        "likes" => {
                            assert_eq!(edge.from_node, person1.id);
                            assert_eq!(edge.to_node, person3.id);
                        }
                        "follows" => {
                            assert_eq!(edge.from_node, person2.id);
                            assert_eq!(edge.to_node, person3.id);
                        }
                        _ => panic!("Unexpected edge label"),
                    }
                }
            }
            _ => panic!("Expected EdgeArray value"),
        }
    }

    #[test]
    fn test_v_empty_graph() {
        let (storage, _temp_dir) = setup_test_db();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v();

        // Check that the node array is empty
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 0);
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    #[test]
    fn test_e_empty_graph() {
        let (storage, _temp_dir) = setup_test_db();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.e();

        // Check that the edge array is empty
        match &traversal.current_step {
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 0);
            }
            _ => panic!("Expected EdgeArray value"),
        }
    }

    #[test]
    fn test_v_nodes_without_edges() {
        let (storage, _temp_dir) = setup_test_db();

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v();

        // Check that the node array contains the two nodes
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 2);
                let node_ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
                assert!(node_ids.contains(&person1.id));
                assert!(node_ids.contains(&person2.id));
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    #[test]
    fn test_add_v() {
        let (storage, _temp_dir) = setup_test_db();
        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);

        traversal.add_v("person", props! {});

        // Check that the current step contains a single node
        match &traversal.current_step {
            TraversalValue::NodeArray(node) => {
                assert_eq!(node.first().unwrap().label, "person");
            }
            _ => panic!("Expected SingleNode value"),
        }
    }

    #[test]
    fn test_add_e() {
        let (storage, _temp_dir) = setup_test_db();

        let node1 = storage.create_node("person", props!()).unwrap();
        let node2 = storage.create_node("person", props!()).unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.add_e("knows", &node1.id, &node2.id, props!());

        // Check that the current step contains a single edge
        match &traversal.current_step {
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].label, "knows");
                assert_eq!(edges[0].from_node, node1.id);
                assert_eq!(edges[0].to_node, node2.id);
            }
            _ => panic!("Expected SingleEdge value"),
        }
    }

    #[test]
    fn test_out() {
        let (storage, _temp_dir) = setup_test_db();

        // Create graph: (person1)-[knows]->(person2)-[knows]->(person3)
        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        let person3 = storage.create_node("person", props!()).unwrap();

        storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge("knows", &person2.id, &person3.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person1.clone()));
        // Traverse from person1 to person2
        traversal.out("knows");

        // Check that current step is at person2
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person2.id);
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    #[test]
    fn test_out_e() {
        let (storage, _temp_dir) = setup_test_db();

        // Create graph: (person1)-[knows]->(person2)
        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();

        let edge = storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person1.clone()));
        // Traverse from person1 to person2
        traversal.out_e("knows");

        // Check that current step is at the edge between person1 and person2
        match &traversal.current_step {
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].id, edge.id);
                assert_eq!(edges[0].label, "knows");
            }
            _ => panic!("Expected EdgeArray value"),
        }
    }

    #[test]
    fn test_in() {
        let (storage, _temp_dir) = setup_test_db();

        // Create graph: (person1)-[knows]->(person2)
        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();

        storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person2.clone()));
        // Traverse from person2 to person1
        traversal.in_("knows");


        // Check that current step is at person1
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person1.id);
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    #[test]
    fn test_in_e() {
        let (storage, _temp_dir) = setup_test_db();

        // Create test graph: (person1)-[knows]->(person2)
        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();

        let edge = storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person2.clone()));
        // Traverse from person2 to person1
        traversal.in_e("knows");

        // Check that current step is at the edge between person1 and person2
        match &traversal.current_step {
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].id, edge.id);
                assert_eq!(edges[0].label, "knows");
            }
            _ => panic!("Expected EdgeArray value"),
        }
    }

    #[test]
    fn test_traversal_validation() {
        let (storage, _temp_dir) = setup_test_db();
        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);

        let node1 = storage.create_node("person", props!()).unwrap();
        let node2 = storage.create_node("person", props!()).unwrap();
        let edge = storage
            .create_edge("knows", &node1.id, &node2.id, props!())
            .unwrap();
        traversal.current_step = TraversalValue::from(edge);

        assert!(traversal.check_is_valid_node_traversal("test").is_err());

        traversal.current_step = TraversalValue::from(node1);
        assert!(traversal.check_is_valid_edge_traversal("test").is_err());
    }

    #[test]
    fn test_complex_traversal() {
        let (storage, _temp_dir) = setup_test_db();

        // Graph structure:
        // (person1)-[knows]->(person2)-[likes]->(person3)
        //     ^                                     |
        //     |                                     |
        //     +-------<------[follows]------<-------+

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        let person3 = storage.create_node("person", props!()).unwrap();

        storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge("likes", &person2.id, &person3.id, props!())
            .unwrap();
        storage
            .create_edge("follows", &person3.id, &person1.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person1.clone()));

        // Traverse from person1 to person2
        traversal.out("knows");

        // Check that current step is at person2
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person2.id);
            }
            _ => panic!("Expected NodeArray value"),
        }

        // Traverse from person2 to person3
        traversal.out("likes");

        // Check that current step is at person3
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person3.id);
            }
            _ => panic!("Expected NodeArray value"),
        }

        // Traverse from person3 to person1
        traversal.out("follows");

        // Check that current step is at person1
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person1.id);
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    #[test]
    fn test_count_single_node() {
        let (storage, _temp_dir) = setup_test_db();
        let person = storage.create_node("person", props!()).unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person));
        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 1);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_count_node_array() {
        let (storage, _temp_dir) = setup_test_db();
        let _ = storage.create_node("person", props!()).unwrap();
        let _ = storage.create_node("person", props!()).unwrap();
        let _ = storage.create_node("person", props!()).unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v(); // Get all nodes
        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 3);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_count_mixed_steps() {
        let (storage, _temp_dir) = setup_test_db();

        // Create a graph with multiple paths
        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        let person3 = storage.create_node("person", props!()).unwrap();

        storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge("knows", &person1.id, &person3.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person1));
        traversal.out("knows"); // Should have 2 nodes (person2 and person3)

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 2);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_range_subset() {
        let (storage, _temp_dir) = setup_test_db();

        // Create multiple nodes
        let _: Vec<Node> = (0..5)
            .map(|_| storage.create_node("person", props!()).unwrap())
            .collect();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v(); // Get all nodes
        traversal.range(1, 3); // Take nodes at index 1 and 2

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 2);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_range_chaining() {
        let (storage, _temp_dir) = setup_test_db();

        // Create graph: (p1)-[knows]->(p2)-[knows]->(p3)-[knows]->(p4)-[knows]->(p5)
        let nodes: Vec<Node> = (0..5)
            .map(|i| {
                storage
                    .create_node("person", props! { "name" => i })
                    .unwrap()
            })
            .collect();

        // Create edges connecting nodes sequentially
        for i in 0..4 {
            storage
                .create_edge("knows", &nodes[i].id, &nodes[i + 1].id, props!())
                .unwrap();
        }

        storage
            .create_edge("knows", &nodes[4].id, &nodes[0].id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v(); // Get all nodes
        traversal.range(0, 3); // Take first 3 nodes
        traversal.out("knows"); // Get their outgoing nodes

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 3);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_range_empty() {
        let (storage, _temp_dir) = setup_test_db();
        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v();
        traversal.range(0, 0);
        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 0);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_count_empty() {
        let (storage, _temp_dir) = setup_test_db();
        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 0);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_v_from_id() {
        let (storage, _temp_dir) = setup_test_db();

        // Create a test node
        let person = storage.create_node("person", props!()).unwrap();
        let node_id = person.id.clone();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v_from_id(&node_id);
        // Check that the current step contains the correct single node
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, node_id);
                assert_eq!(nodes[0].label, "person");
            }
            _ => panic!("Expected SingleNode value"),
        }
    }

    #[test]
    fn test_v_from_id_with_traversal() {
        let (storage, _temp_dir) = setup_test_db();

        // Create test graph: (person1)-[knows]->(person2)
        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v_from_id(&person1.id).out("knows");

        // Check that traversal reaches person2
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person2.id);
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    #[test]
    fn test_e_from_id() {
        let (storage, _temp_dir) = setup_test_db();

        // Create test graph and edge
        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        let edge = storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();
        let edge_id = edge.id.clone();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.e_from_id(&edge_id);

        // Check that the current step contains the correct single edge
        match &traversal.current_step {
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].id, edge_id);
                assert_eq!(edges[0].label, "knows");
                assert_eq!(edges[0].from_node, person1.id);
                assert_eq!(edges[0].to_node, person2.id);
            }
            _ => panic!("Expected SingleEdge value"),
        }
    }

    #[test]
    fn test_v_from_id_nonexistent() {
        let (storage, _temp_dir) = setup_test_db();

        let _ = TraversalBuilder::new(&storage, TraversalValue::Empty);
        let result = storage.get_node("nonexistent_id");
        assert!(result.is_err());

        if let Err(e) = result {
            matches!(e, GraphError::NodeNotFound);
        }
    }

    #[test]
    fn test_e_from_id_nonexistent() {
        let (storage, _temp_dir) = setup_test_db();

        let _ = TraversalBuilder::new(&storage, TraversalValue::Empty);
        let result = storage.get_edge("nonexistent_id");
        assert!(result.is_err());

        if let Err(e) = result {
            matches!(e, GraphError::EdgeNotFound);
        }
    }

    #[test]
    fn test_v_from_id_chain_operations() {
        let (storage, _temp_dir) = setup_test_db();

        // Create test graph: (person1)-[knows]->(person2)-[likes]->(person3)
        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        let person3 = storage.create_node("person", props!()).unwrap();

        storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge("likes", &person2.id, &person3.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v_from_id(&person1.id).out("knows").out("likes");

        // Check that the chain of traversals reaches person3
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person3.id);
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    #[test]
    fn test_e_from_id_chain_operations() {
        let (storage, _temp_dir) = setup_test_db();

        // Create test graph and edges
        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        let edge = storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        let count_before = traversal.e_from_id(&edge.id).count();

        if let TraversalValue::Count(count) = &count_before.current_step {
            assert_eq!(count.value(), 1);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_filter_nodes() {
        let (storage, _temp_dir) = setup_test_db();

        // Create nodes with different properties
        let _ = storage
            .create_node("person", props! { "age" => 25 })
            .unwrap();
        let _ = storage
            .create_node("person", props! { "age" => 30 })
            .unwrap();
        let person3 = storage
            .create_node("person", props! { "age" => 35 })
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v();

        // Filter nodes with age > 30
        traversal.filter_nodes(|val| {
            if let Some(value) = val.check_property("age") {
                match value {
                    Value::Float(age) => Ok(*age > 30.0),
                    Value::Integer(age) => Ok(*age > 30),

                    _ => Err(GraphError::TraversalError("Invalid type".to_string())),
                }
            } else {
                Err(GraphError::TraversalError("No age property".to_string()))
            }
        });
        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person3.id);
            }
            _ => panic!("Expected Node value"),
        }

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 1);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_filter_macro_single_argument() {
        let (storage, _temp_dir) = setup_test_db();

        let _ = storage
            .create_node("person", props! { "name" => "Alice" })
            .unwrap();
        let _ = storage
            .create_node("person", props! { "name" => "Bob" })
            .unwrap();

        fn has_name(val: &Node) -> Result<bool, GraphError> {
            return Ok(val.check_property("name").is_some());
        }

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v().filter_nodes(has_name);

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 2);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_filter_macro_multiple_arguments() {
        let (storage, _temp_dir) = setup_test_db();

        let _ = storage
            .create_node("person", props! { "age" => 25 })
            .unwrap();
        let person2 = storage
            .create_node("person", props! { "age" => 30 })
            .unwrap();

        fn age_greater_than(val: &Node, min_age: i32) -> Result<bool, GraphError> {
            if let Some(value) = val.check_property("age") {
                match value {
                    Value::Float(age) => Ok(*age > min_age as f64),
                    Value::Integer(age) => Ok(*age > min_age),
                    _ => Err(GraphError::TraversalError("Invalid type".to_string())),
                }
            } else {
                Err(GraphError::TraversalError("Invalid node".to_string()))
            }
        }

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v();
        traversal.filter_nodes(|node| age_greater_than(node, 27));

        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person2.id);
            }
            _ => panic!("Expected Node value"),
        }

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 1);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_filter_edges() {
        let (storage, _temp_dir) = setup_test_db();

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();

        let _ = storage
            .create_edge(
                "knows",
                &person1.id,
                &person2.id,
                props! { "since" => 2020 },
            )
            .unwrap();
        let edge2 = storage
            .create_edge(
                "knows",
                &person2.id,
                &person1.id,
                props! { "since" => 2022 },
            )
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.e();

        fn recent_edge(val: &Edge, year: i32) -> Result<bool, GraphError> {
            if let Some(value) = val.check_property("since") {
                match value {
                    Value::Integer(since) => return Ok(*since > year),
                    Value::Float(since) => return Ok(*since > year as f64),
                    _ => return Err(GraphError::TraversalError("Invalid type".to_string())),
                }
            }
            Err(GraphError::TraversalError("Invalid edge".to_string()))
        }

        traversal.filter_edges(|edge| recent_edge(edge, 2021));

        match &traversal.current_step {
            // TraversalValue::SingleEdge(edge) => {
            //     assert_eq!(edge.id, edge2.id);
            // }
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].id, edge2.id);
            }
            _ => panic!("Expected Edge value"),
        }

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 1);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_filter_empty_result() {
        let (storage, _temp_dir) = setup_test_db();

        let _ = storage
            .create_node("person", props! { "age" => 25 })
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v();

        // Filter with a condition that no nodes satisfy
        traversal.filter_nodes(|val| {
            if let Some(value) = val.check_property("age") {
                match value {
                    Value::Integer(age) => return Ok(*age > 100),
                    Value::Float(age) => return Ok(*age > 100.0),
                    _ => return Err(GraphError::TraversalError("Invalid type".to_string())),
                }
            }
            Err(GraphError::TraversalError("Invalid node".to_string()))
        });
        if let TraversalValue::NodeArray(nodes) = &traversal.current_step {
            assert!(nodes.is_empty());
        } else {
            panic!("Expected NodeArray value");
        }

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 0);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_filter_chain() {
        let (storage, _temp_dir) = setup_test_db();

        let _ = storage
            .create_node("person", props! { "age" => 25, "name" => "Alice" })
            .unwrap();
        let person2 = storage
            .create_node("person", props! { "age" => 30, "name" => "Bob" })
            .unwrap();
        let _ = storage
            .create_node("person", props! { "age" => 35 })
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.v();

        fn has_name(val: &Node) -> Result<bool, GraphError> {
            return Ok(val.check_property("name").is_some());
        }

        fn age_greater_than(val: &Node, min_age: i32) -> Result<bool, GraphError> {
            if let Some(value) = val.check_property("age") {
                match value {
                    Value::Float(age) => Ok(*age > min_age as f64),
                    Value::Integer(age) => Ok(*age > min_age),
                    _ => Err(GraphError::TraversalError("Invalid type".to_string())),
                }
            } else {
                Err(GraphError::TraversalError("Invalid node".to_string()))
            }
        }

        traversal
            .filter_nodes(has_name)
            .filter_nodes(|val| age_greater_than(val, 27));

        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person2.id);
            }
            _ => panic!("Expected Node value"),
        }

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 1);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_in_v() {
        let (storage, _temp_dir) = setup_test_db();

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();

        let edge = storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.e_from_id(&edge.id).in_v();

        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person2.id);
            }
            _ => panic!("Expected SingleNode value"),
        }
    }

    #[test]
    fn test_out_v() {
        let (storage, _temp_dir) = setup_test_db();

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();

        let edge = storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.e_from_id(&edge.id).out_v();

        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person1.id);
            }
            _ => panic!("Expected SingleNode value"),
        }
    }

    #[test]
    fn test_both() {
        let (storage, _temp_dir) = setup_test_db();

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();
        let person3 = storage.create_node("person", props!()).unwrap();

        storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge("knows", &person2.id, &person3.id, props!())
            .unwrap();
        storage
            .create_edge("knows", &person3.id, &person2.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person2.clone()));
        traversal.both("knows");

        let nds = match_node_array(&traversal.current_step);
        let nodes = nds.iter().map(|n| n.id.clone()).collect::<Vec<String>>();

        assert_eq!(nodes.len(), 3);
        assert!(nodes.contains(&person1.id));
        assert!(nodes.contains(&person3.id));
    }

    #[test]
    fn test_both_e() {
        let (storage, _temp_dir) = setup_test_db();

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();

        let edge1 = storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();
        let edge2 = storage
            .create_edge("likes", &person2.id, &person1.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person2.clone()));
        traversal.both_e("knows");

        match &traversal.current_step {
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].id, edge1.id);
            }
            _ => panic!("Expected EdgeArray value"),
        }

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::from(person2.clone()));
        traversal.both_e("likes");

        match &traversal.current_step {
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].id, edge2.id);
            }
            _ => panic!("Expected EdgeArray value"),
        }
    }

    #[test]
    fn test_both_v() {
        let (storage, _temp_dir) = setup_test_db();

        let person1 = storage.create_node("person", props!()).unwrap();
        let person2 = storage.create_node("person", props!()).unwrap();

        let edge = storage
            .create_edge("knows", &person1.id, &person2.id, props!())
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, TraversalValue::Empty);
        traversal.e_from_id(&edge.id).both_v();

        match &traversal.current_step {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 2);
                let node_ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
                assert!(node_ids.contains(&person1.id));
                assert!(node_ids.contains(&person2.id));
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    fn match_node_array(value: &TraversalValue) -> Vec<Node> {
        match value {
            TraversalValue::NodeArray(nodes) => nodes.clone(),
            _ => vec![],
        }
    }
}
