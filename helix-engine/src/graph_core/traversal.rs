use crate::{
    graph_core::{
        count::Count,
        traversal_steps::{SourceTraversalSteps, TraversalMethods, TraversalSteps},
        traversal_value::TraversalValue,
    },
    props,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use protocol::Node;
use std::collections::HashMap;

pub struct TraversalBuilder<'a> {
    pub variables: HashMap<String, TraversalValue>,
    pub current_step: Vec<TraversalValue>,
    pub storage: &'a HelixGraphStorage,
}

impl<'a> TraversalBuilder<'a> {
    pub fn new(storage: &'a HelixGraphStorage, start_nodes: Vec<Node>) -> Self {
        let builder = Self {
            variables: HashMap::from_iter(props!()),
            current_step: vec![TraversalValue::NodeArray(start_nodes)],
            storage,
        };
        builder
    }

    pub fn check_is_valid_node_traversal(&self, function_name: &str) -> Result<(), GraphError> {
        match matches!(
            self.current_step[0],
            TraversalValue::NodeArray(_) | TraversalValue::SingleNode(_)
        ) {
            true => Ok(()),
            false => Err(GraphError::TraversalError(format!(
                "The traversal step {:?}, is not a valid traversal from an edge. 
                The current step should be a node.",
                function_name
            ))),
        }
    }

    pub fn check_is_valid_edge_traversal(&self, function_name: &str) -> Result<(), GraphError> {
        match matches!(
            self.current_step[0],
            TraversalValue::EdgeArray(_) | TraversalValue::SingleEdge(_)
        ) {
            true => Ok(()),
            false => Err(GraphError::TraversalError(format!(
                "The traversal step {:?}, is not a valid traversal from a node. 
                The current step should be an edge",
                function_name
            ))),
        }
    }
}

impl<'a> SourceTraversalSteps for TraversalBuilder<'a> {
    fn v(&mut self) -> &mut Self {
        let nodes = self.storage.get_all_nodes().unwrap(); // TODO: Handle error
        self.current_step = vec![TraversalValue::NodeArray(nodes)];
        self
    }

    fn e(&mut self) -> &mut Self {
        let edges = self.storage.get_all_edges().unwrap(); // TODO: Handle error
        self.current_step = vec![TraversalValue::EdgeArray(edges)];
        self
    }

    fn add_v(&mut self, node_label: &str) -> &mut Self {
        let node = self.storage.create_node(node_label, props!()).unwrap(); // TODO: Handle error
        self.current_step = vec![TraversalValue::SingleNode(node)];
        self
    }

    fn add_e(
        &mut self,
        storage: &HelixGraphStorage,
        edge_label: &str,
        from_id: &str,
        to_id: &str,
    ) -> &mut Self {
        let edge = storage
            .create_edge(edge_label, from_id, to_id, props!())
            .unwrap(); // TODO: Handle error
        self.current_step = vec![TraversalValue::SingleEdge(edge)];
        self
    }

    fn v_from_id(&mut self, node_id: &str) -> &mut Self {
        let node = self.storage.get_node(node_id).unwrap(); // TODO: Handle error
        self.current_step = vec![TraversalValue::SingleNode(node)];
        self
    }

    fn e_from_id(&mut self, edge_id: &str) -> &mut Self {
        let edge = self.storage.get_edge(edge_id).unwrap(); // TODO: Handle error
        self.current_step = vec![TraversalValue::SingleEdge(edge)];
        self
    }
}

impl<'a> TraversalSteps for TraversalBuilder<'a> {
    fn out(&mut self, edge_label: &str) -> &mut Self {
        self.check_is_valid_node_traversal("out").unwrap(); // TODO: Handle error

        let mut new_current = Vec::with_capacity(self.current_step.len());
        for element in &self.current_step {
            match element {
                TraversalValue::NodeArray(nodes) => {
                    // let mut new_steps = Vec::with_capacity(nodes.len() * self.current_step.len());
                    for node in nodes {
                        let nodes = self.storage.get_out_nodes(&node.id, edge_label).unwrap();
                        match nodes.is_empty() {
                            false => new_current.push(TraversalValue::NodeArray(nodes)),
                            true => new_current.push(TraversalValue::Empty),
                        }
                    }
                }
                TraversalValue::SingleNode(node) => {
                    let nodes = self.storage.get_out_nodes(&node.id, edge_label).unwrap();
                    match nodes.is_empty() {
                        false => new_current.push(TraversalValue::NodeArray(nodes)),
                        true => new_current.push(TraversalValue::Empty),
                    }
                }
                _ => unreachable!(),
            };
        }
        self.current_step = new_current;
        self
    }

    fn out_e(&mut self, edge_label: &str) -> &mut Self {
        self.check_is_valid_node_traversal("out_e").unwrap(); // TODO: Handle error
        let mut new_current = Vec::with_capacity(self.current_step.len());
        for element in &self.current_step {
            match element {
                TraversalValue::NodeArray(nodes) => {
                    // let mut new_steps = Vec::with_capacity(nodes.len() * self.current_step.len());
                    for node in nodes {
                        let edges = self.storage.get_out_edges(&node.id, edge_label).unwrap();
                        match edges.is_empty() {
                            false => new_current.push(TraversalValue::EdgeArray(edges)),
                            true => new_current.push(TraversalValue::Empty),
                        }
                    }
                }
                TraversalValue::SingleNode(node) => {
                    let edges = self.storage.get_out_edges(&node.id, edge_label).unwrap();
                    match edges.is_empty() {
                        false => new_current.push(TraversalValue::EdgeArray(edges)),
                        true => new_current.push(TraversalValue::Empty),
                    }
                }
                _ => unreachable!(),
            };
        }
        self.current_step = new_current;
        self
    }

    fn in_(&mut self, edge_label: &str) -> &mut Self {
        self.check_is_valid_node_traversal("in_").unwrap(); // TODO: Handle error
        let mut new_current = Vec::with_capacity(self.current_step.len());
        for element in &self.current_step {
            match element {
                TraversalValue::NodeArray(nodes) => {
                    // let mut new_steps = Vec::with_capacity(nodes.len() * self.current_step.len());
                    for node in nodes {
                        let nodes = self.storage.get_in_nodes(&node.id, edge_label).unwrap();
                        match nodes.is_empty() {
                            false => new_current.push(TraversalValue::NodeArray(nodes)),
                            true => new_current.push(TraversalValue::Empty),
                        }
                    }
                }
                TraversalValue::SingleNode(node) => {
                    let nodes = self.storage.get_in_nodes(&node.id, edge_label).unwrap();
                    match nodes.is_empty() {
                        false => new_current.push(TraversalValue::NodeArray(nodes)),
                        true => new_current.push(TraversalValue::Empty),
                    }
                }
                _ => unreachable!(),
            };
        }
        self.current_step = new_current;
        self
    }

    fn in_e(&mut self, edge_label: &str) -> &mut Self {
        self.check_is_valid_node_traversal("in_e").unwrap(); // TODO: Handle error
        let mut new_current = Vec::with_capacity(self.current_step.len());
        for element in &self.current_step {
            match element {
                TraversalValue::NodeArray(nodes) => {
                    // let mut new_steps = Vec::with_capacity(nodes.len() * self.current_step.len());
                    for node in nodes {
                        let edges = self.storage.get_in_edges(&node.id, edge_label).unwrap();
                        match edges.is_empty() {
                            false => new_current.push(TraversalValue::EdgeArray(edges)),
                            true => new_current.push(TraversalValue::Empty),
                        }
                    }
                }
                TraversalValue::SingleNode(node) => {
                    let edges = self.storage.get_in_edges(&node.id, edge_label).unwrap();
                    match edges.is_empty() {
                        false => new_current.push(TraversalValue::EdgeArray(edges)),
                        true => new_current.push(TraversalValue::Empty),
                    }
                }
                _ => unreachable!(),
            };
        }
        self.current_step = new_current;
        self
    }
}

impl<'a> TraversalMethods for TraversalBuilder<'a> {
    fn count(&mut self) -> Count {
        Count::new(self.current_step.iter().flatten().count())
    }
    fn range(&mut self, start: usize, end: usize) -> &mut Self {
        let elements = self.current_step.iter().flatten().collect::<Vec<_>>();
        if 0 < elements.len() {
            self.current_step = elements[start..end].to_vec();
            self
        } else {
            self
        }
    }

    fn filter<F>(&mut self, predicate: F) -> &mut Self
    where
        F: Fn(&TraversalValue) -> bool,
    {
        self.current_step = self
            .current_step
            .iter()
            .flatten()
            .filter(|item| predicate(&item)) // TODO: HANDLE ERROR
            .collect::<Vec<_>>();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::props;
    use protocol::{Node, Value};
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v();

        // Check that the node array contains all nodes
        match &traversal.current_step[0] {
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.e();

        // Check that the edge array contains the three edges
        match &traversal.current_step[0] {
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v();

        // Check that the node array is empty
        match &traversal.current_step[0] {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 0);
            }
            _ => panic!("Expected NodeArray value"),
        }
    }

    #[test]
    fn test_e_empty_graph() {
        let (storage, _temp_dir) = setup_test_db();

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.e();

        // Check that the edge array is empty
        match &traversal.current_step[0] {
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v();

        // Check that the node array contains the two nodes
        match &traversal.current_step[0] {
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
        let mut traversal = TraversalBuilder::new(&storage, vec![]);

        traversal.add_v("person");

        // Check that the current step contains a single node
        match &traversal.current_step[0] {
            TraversalValue::SingleNode(node) => {
                assert_eq!(node.label, "person");
            }
            _ => panic!("Expected SingleNode value"),
        }
    }

    #[test]
    fn test_add_e() {
        let (storage, _temp_dir) = setup_test_db();

        let node1 = storage.create_node("person", props!()).unwrap();
        let node2 = storage.create_node("person", props!()).unwrap();

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.add_e(&storage, "knows", &node1.id, &node2.id);

        // Check that the current step contains a single edge
        match &traversal.current_step[0] {
            TraversalValue::SingleEdge(edge) => {
                assert_eq!(edge.label, "knows");
                assert_eq!(edge.from_node, node1.id);
                assert_eq!(edge.to_node, node2.id);
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

        let mut traversal = TraversalBuilder::new(&storage, vec![person1.clone()]);
        // Traverse from person1 to person2
        traversal.out("knows");

        // Check that current step is at person2
        match &traversal.current_step[0] {
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

        let mut traversal = TraversalBuilder::new(&storage, vec![person1.clone()]);
        // Traverse from person1 to person2
        traversal.out_e("knows");

        // Check that current step is at the edge between person1 and person2
        match &traversal.current_step[0] {
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

        let mut traversal = TraversalBuilder::new(&storage, vec![person2.clone()]);

        // Traverse from person2 to person1
        traversal.in_("knows");

        // Check that current step is at person1
        match &traversal.current_step[0] {
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

        let mut traversal = TraversalBuilder::new(&storage, vec![person2.clone()]);
        // Traverse from person2 to person1
        traversal.in_e("knows");

        // Check that current step is at the edge between person1 and person2
        match &traversal.current_step[0] {
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
        let mut traversal = TraversalBuilder::new(&storage, vec![]);

        let node1 = storage.create_node("person", props!()).unwrap();
        let node2 = storage.create_node("person", props!()).unwrap();
        let edge = storage
            .create_edge("knows", &node1.id, &node2.id, props!())
            .unwrap();
        traversal.current_step = vec![TraversalValue::SingleEdge(edge)];

        assert!(traversal.check_is_valid_node_traversal("test").is_err());

        traversal.current_step = vec![TraversalValue::SingleNode(node1)];
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

        let mut traversal = TraversalBuilder::new(&storage, vec![person1.clone()]);

        // Traverse from person1 to person2
        traversal.out("knows");

        // Check that current step is at person2
        match &traversal.current_step[0] {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person2.id);
            }
            _ => panic!("Expected NodeArray value"),
        }

        // Traverse from person2 to person3
        traversal.out("likes");

        // Check that current step is at person3
        match &traversal.current_step[0] {
            TraversalValue::NodeArray(nodes) => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].id, person3.id);
            }
            _ => panic!("Expected NodeArray value"),
        }

        // Traverse from person3 to person1
        traversal.out("follows");

        // Check that current step is at person1
        match &traversal.current_step[0] {
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

        let mut traversal = TraversalBuilder::new(&storage, vec![person]);
        assert_eq!(traversal.count(), 1);
    }

    #[test]
    fn test_count_node_array() {
        let (storage, _temp_dir) = setup_test_db();
        let _ = storage.create_node("person", props!()).unwrap();
        let _ = storage.create_node("person", props!()).unwrap();
        let _ = storage.create_node("person", props!()).unwrap();

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v(); // Get all nodes
        assert_eq!(traversal.count(), 3);
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

        let mut traversal = TraversalBuilder::new(&storage, vec![person1.clone()]);
        traversal.out("knows"); // Should have 2 nodes (person2 and person3)
        assert_eq!(traversal.count(), 2);
    }

    #[test]
    fn test_range_subset() {
        let (storage, _temp_dir) = setup_test_db();

        // Create multiple nodes
        let _: Vec<Node> = (0..5)
            .map(|_| storage.create_node("person", props!()).unwrap())
            .collect();

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v(); // Get all nodes
        traversal.range(1, 3); // Take nodes at index 1 and 2

        assert_eq!(traversal.count(), 2);
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v(); // Get all nodes

        println!("V: {:?}", traversal.current_step);
        println!();
        traversal.range(0, 3); // Take first 3 nodes
        println!("R: {:?}", traversal.current_step);
        println!();
        traversal.out("knows"); // Get their outgoing nodes
        println!("O: {:?}", traversal.current_step);

        assert_eq!(traversal.count(), 3);
    }

    #[test]
    fn test_range_empty() {
        let (storage, _temp_dir) = setup_test_db();
        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v();
        traversal.range(0, 0);
        assert_eq!(traversal.count(), 0);
    }

    #[test]
    fn test_count_empty() {
        let (storage, _temp_dir) = setup_test_db();
        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        assert_eq!(traversal.count(), 0);
    }

    #[test]
    fn test_v_from_id() {
        let (storage, _temp_dir) = setup_test_db();

        // Create a test node
        let person = storage.create_node("person", props!()).unwrap();
        let node_id = person.id.clone();

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v_from_id(&node_id);

        // Check that the current step contains the correct single node
        match &traversal.current_step[0] {
            TraversalValue::SingleNode(node) => {
                assert_eq!(node.id, node_id);
                assert_eq!(node.label, "person");
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v_from_id(&person1.id).out("knows");

        // Check that traversal reaches person2
        match &traversal.current_step[0] {
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.e_from_id(&edge_id);

        // Check that the current step contains the correct single edge
        match &traversal.current_step[0] {
            TraversalValue::SingleEdge(e) => {
                assert_eq!(e.id, edge_id);
                assert_eq!(e.label, "knows");
                assert_eq!(e.from_node, person1.id);
                assert_eq!(e.to_node, person2.id);
            }
            _ => panic!("Expected SingleEdge value"),
        }
    }

    #[test]
    fn test_v_from_id_nonexistent() {
        let (storage, _temp_dir) = setup_test_db();

        let _ = TraversalBuilder::new(&storage, vec![]);
        let result = storage.get_node("nonexistent_id");
        assert!(result.is_err());

        if let Err(e) = result {
            matches!(e, GraphError::NodeNotFound);
        }
    }

    #[test]
    fn test_e_from_id_nonexistent() {
        let (storage, _temp_dir) = setup_test_db();

        let _ = TraversalBuilder::new(&storage, vec![]);
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v_from_id(&person1.id).out("knows").out("likes");

        // Check that the chain of traversals reaches person3
        println!("{:?}: {:?}", traversal.current_step, person3.id);
        match &traversal.current_step[0] {
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        let count_before = traversal.e_from_id(&edge.id).count();

        assert_eq!(count_before, 1, "Expected single edge in traversal");
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v();

        // Filter nodes with age > 30
        traversal.filter(|val| {
            if let TraversalValue::SingleNode(node) = val {
                if let Some(Value::Integer(age)) = node.properties.get("age") {
                    return *age > 30;
                }
            }
            false
        });

        assert_eq!(traversal.count(), 1);
        match &traversal.current_step[0] {
            TraversalValue::SingleNode(node) => {
                assert_eq!(node.id, person3.id);
            }
            _ => panic!("Expected Node value"),
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

        fn has_name(val: &TraversalValue) -> bool {
            if let TraversalValue::SingleNode(node) = val {
                return node.properties.contains_key("name");
            }
            false
        }

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v().filter(has_name);

        assert_eq!(traversal.count(), 2);
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

        fn age_greater_than(val: &TraversalValue, min_age: i32) -> bool {
            if let TraversalValue::SingleNode(node) = val {
                if let Some(Value::Integer(age)) = node.properties.get("age") {
                    return *age > min_age;
                }
            }
            false
        }

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v();
        traversal.filter(|node| age_greater_than(node, 27));

        assert_eq!(traversal.count(), 1);
        match &traversal.current_step[0] {
            TraversalValue::SingleNode(node) => {
                assert_eq!(node.id, person2.id);
            }
            _ => panic!("Expected Node value"),
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.e();

        fn recent_edge(val: &TraversalValue, year: i32) -> bool {
            if let TraversalValue::SingleEdge(edge) = val {
                if let Some(Value::Integer(since)) = edge.properties.get("since") {
                    return *since > year;
                }
            }
            false
        }

        traversal.filter(|edge| recent_edge(edge, 2021));

        assert_eq!(traversal.count(), 1);
        match &traversal.current_step[0] {
            TraversalValue::SingleEdge(edge) => {
                assert_eq!(edge.id, edge2.id);
            }
            _ => panic!("Expected Edge value"),
        }
    }

    #[test]
    fn test_filter_empty_result() {
        let (storage, _temp_dir) = setup_test_db();

        let _ = storage
            .create_node("person", props! { "age" => 25 })
            .unwrap();

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v();

        // Filter with a condition that no nodes satisfy
        traversal.filter(|val| {
            if let TraversalValue::SingleNode(node) = val {
                if let Some(Value::Integer(age)) = node.properties.get("age") {
                    return *age > 100;
                }
            }
            false
        });

        assert_eq!(traversal.count(), 0);
        assert!(traversal.current_step.is_empty());
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

        let mut traversal = TraversalBuilder::new(&storage, vec![]);
        traversal.v();

        fn has_name(val: &TraversalValue) -> bool {
            if let TraversalValue::SingleNode(node) = val {
                return node.properties.contains_key("name");
            }
            false
        }

        fn age_greater_than(val: &TraversalValue, min_age: i32) -> bool {
            if let TraversalValue::SingleNode(node) = val {
                if let Some(Value::Integer(age)) = node.properties.get("age") {
                    return *age > min_age;
                }
            }
            false
        }

        traversal
            .filter(has_name)
            .filter(|val| age_greater_than(val, 27));

        assert_eq!(traversal.count(), 1);
        match &traversal.current_step[0] {
            TraversalValue::SingleNode(node) => {
                assert_eq!(node.id, person2.id);
                assert_eq!(
                    node.properties.get("name"),
                    Some(&Value::String("Bob".to_string()))
                );
            }
            _ => panic!("Expected Node value"),
        }
    }
}
