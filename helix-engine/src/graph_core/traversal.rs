use crate::{
    graph_core::traversal_steps::{
        SourceTraversalSteps, TraversalMethods, TraversalSearchMethods, TraversalSteps,
    },
    props,
    storage_core::{
        storage_core::HelixGraphStorage,
        storage_methods::{SearchMethods, StorageMethods},
        txn_context::TransactionContext,
    },
    types::GraphError,
};
use core::panic;
use heed3::{RoTxn, RwTxn};
use protocol::{
    count::Count, filterable::Filterable, traversal_value::TraversalValue, value::Value, Edge, Node,
};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::traversal_steps::TraversalBuilderMethods;

pub struct TraversalBuilder<'a> {
    pub variables: HashMap<String, TraversalValue>,
    pub current_step: TraversalValue,
    pub storage: Arc<HelixGraphStorage>,
    pub error: Option<GraphError>,
    pub rtxn: Option<RoTxn<'a>>,
    pub wtxn: Option<RwTxn<'a>>,
}

impl<'a> TraversalBuilder<'a> {
    pub fn new(
        storage: Arc<HelixGraphStorage>,
        start_nodes: TraversalValue,
        rtxn: Option<RoTxn<'a>>,
        wtxn: Option<RwTxn<'a>>,
    ) -> Self {
        Self {
            variables: HashMap::new(),
            current_step: start_nodes,
            storage,
            error: None,
            rtxn,
            wtxn,
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

    #[inline(always)]
    fn store_error(&mut self, err: GraphError) {
        if let GraphError::Empty = err {
            return;
        }
        if self.error.is_none() {
            self.error = Some(err);
        }
    }
}

impl<'a> SourceTraversalSteps for TraversalBuilder<'a> {
    fn v(&mut self) -> &mut Self {
        match self.storage.get_all_nodes(self.rtxn.as_ref().unwrap()) {
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
        match self.storage.get_all_edges(self.rtxn.as_ref().unwrap()) {
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
        match self
            .storage
            .create_node(self.wtxn.as_mut().unwrap(), node_label, props)
        {
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
        match self.storage.create_edge(
            self.wtxn.as_mut().unwrap(),
            edge_label,
            from_id,
            to_id,
            props,
        ) {
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
        match self.storage.get_node(self.rtxn.as_ref().unwrap(), node_id) {
            Ok(node) => {
                self.current_step = TraversalValue::from(node);
            }
            Err(err) => {
                self.store_error(err);
            }
        }
        self
    }

    fn v_from_ids(&mut self, node_ids: &[String]) -> &mut Self {
        let mut new_current = Vec::with_capacity(node_ids.len());
        for node_id in node_ids {
            match self.storage.get_temp_node(self.rtxn.as_ref().unwrap(), node_id) {
                Ok(node) => new_current.push(node),
                Err(err) => {
                    self.store_error(err);
                }
            }
        }
        if new_current.is_empty() {
            self.current_step = TraversalValue::Empty;
        } else {
            self.current_step = TraversalValue::NodeArray(new_current);
        }
        self
    }

    fn e_from_id(&mut self, edge_id: &str) -> &mut Self {
        match self.storage.get_edge(self.rtxn.as_ref().unwrap(), edge_id) {
            Ok(edge) => {
                self.current_step = TraversalValue::from(edge);
            }
            Err(err) => {
                self.store_error(err);
            }
        }
        self
    }

    fn v_from_types(&mut self, node_labels: &[String]) -> &mut Self {
        match self.storage.get_nodes_by_types(self.rtxn.as_ref().unwrap(), node_labels) {
            Ok(nodes) => {
                self.current_step = TraversalValue::NodeArray(nodes);
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
        let mut e = GraphError::Empty;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self
                    .storage
                    .get_out_nodes(self.rtxn.as_ref().unwrap(), &node.id, edge_label)
                {
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
        let mut e = GraphError::Empty;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self
                    .storage
                    .get_out_edges(self.rtxn.as_ref().unwrap(), &node.id, edge_label)
                {
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
        let mut e = GraphError::Empty;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self
                    .storage
                    .get_in_nodes(self.rtxn.as_ref().unwrap(), &node.id, edge_label)
                {
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
        let mut e = GraphError::Empty;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self
                    .storage
                    .get_in_edges(self.rtxn.as_ref().unwrap(), &node.id, edge_label)
                {
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
        let mut e = GraphError::Empty;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self
                    .storage
                    .get_in_edges(self.rtxn.as_ref().unwrap(), &node.id, edge_label)
                {
                    Ok(in_edges) => {
                        if !in_edges.is_empty() {
                            new_current.extend(in_edges);
                        }
                    }
                    Err(err) => e = err,
                }
                match self
                    .storage
                    .get_out_edges(self.rtxn.as_ref().unwrap(), &node.id, edge_label)
                {
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
        let mut e = GraphError::Empty;
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let mut new_current = Vec::with_capacity(nodes.len());
            for node in nodes {
                match self
                    .storage
                    .get_in_nodes(self.rtxn.as_ref().unwrap(), &node.id, edge_label)
                {
                    Ok(in_nodes) => {
                        if !in_nodes.is_empty() {
                            new_current.extend(in_nodes);
                        }
                    }
                    Err(err) => e = err,
                }
                match self
                    .storage
                    .get_out_nodes(self.rtxn.as_ref().unwrap(), &node.id, edge_label)
                {
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
        let mut e = GraphError::Empty;
        if let TraversalValue::EdgeArray(edges) = &self.current_step {
            let mut new_current = Vec::with_capacity(edges.len());
            for edge in edges {
                match self
                    .storage
                    .get_node(self.rtxn.as_ref().unwrap(), &edge.from_node)
                {
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
        let mut e = GraphError::Empty;
        if let TraversalValue::EdgeArray(edges) = &self.current_step {
            let mut new_current = Vec::with_capacity(edges.len());
            for edge in edges {
                match self
                    .storage
                    .get_node(self.rtxn.as_ref().unwrap(), &edge.to_node)
                {
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
        let mut e = GraphError::Empty;
        if let TraversalValue::EdgeArray(edges) = &self.current_step {
            let mut new_current = Vec::with_capacity(edges.len() * 2);
            for edge in edges {
                match self
                    .storage
                    .get_node(self.rtxn.as_ref().unwrap(), &edge.from_node)
                {
                    Ok(node) => new_current.push(node),
                    Err(err) => e = err,
                }
                match self
                    .storage
                    .get_node(self.rtxn.as_ref().unwrap(), &edge.to_node)
                {
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

    fn mutual(&mut self, edge_label: &str) -> &mut Self {
        let mut e: GraphError = GraphError::Empty;

        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            let result: Vec<Node> = nodes
                .iter()
                .filter_map(|node| {
                    let out = self.storage.get_out_nodes(
                        self.rtxn.as_ref().unwrap(),
                        &node.id,
                        edge_label,
                    );
                    let in_ = self.storage.get_in_nodes(
                        self.rtxn.as_ref().unwrap(),
                        &node.id,
                        edge_label,
                    );

                    match (out, in_) {
                        (Ok(out), Ok(in_)) => {
                            let in_set: HashSet<_> = in_.into_iter().map(|n| n.id).collect();
                            Some(
                                out.into_iter()
                                    .filter(|n| in_set.contains(&n.id))
                                    .collect::<Vec<_>>(),
                            )
                        }
                        (Err(err), _) | (_, Err(err)) => {
                            e = err;

                            None
                        }
                    }
                })
                .flatten()
                .collect();

            self.current_step = TraversalValue::NodeArray(result);
        }
        self.store_error(e);
        self
    }

    fn add_e_from(
        &mut self,
        edge_label: &str,
        from_id: &str,
        props: Vec<(String, Value)>,
    ) -> &mut Self {
        let mut e = GraphError::Empty;
        match &self.current_step {
            TraversalValue::NodeArray(nodes) => {
                let mut new_current = Vec::with_capacity(nodes.len());
                for node in nodes {
                    match self.storage.create_edge(
                        self.wtxn.as_mut().unwrap(),
                        edge_label,
                        from_id,
                        &node.id,
                        props.clone(),
                    ) {
                        Ok(edge) => new_current.push(edge),
                        Err(err) => e = err,
                    }
                }
                self.current_step = TraversalValue::EdgeArray(new_current);
            }
            _ => panic!("Invalid traversal step for add_e_from"),
        }
        self.store_error(e);
        self
    }

    fn add_e_to(
        &mut self,
        edge_label: &str,
        to_id: &str,
        props: Vec<(String, Value)>,
    ) -> &mut Self {
        let mut e = GraphError::Empty;
        match &self.current_step {
            TraversalValue::NodeArray(nodes) => {
                let mut new_current = Vec::with_capacity(nodes.len());
                for node in nodes {
                    match self.storage.create_edge(
                        self.wtxn.as_mut().unwrap(),
                        edge_label,
                        &node.id,
                        to_id,
                        props.clone(),
                    ) {
                        Ok(edge) => new_current.push(edge),
                        Err(err) => e = err,
                    }
                }
                self.current_step = TraversalValue::EdgeArray(new_current);
            }
            _ => panic!("Invalid traversal step for add_e_to"),
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
            _ => panic!("Invalid traversal step for count {:?}", &self.current_step),
        }));
        self
    }
    fn range(&mut self, start: usize, end: usize) -> &mut Self {
        match &self.current_step {
            TraversalValue::NodeArray(nodes) => {
                if nodes.len() == 0 {
                    self.current_step = TraversalValue::Empty;
                } else if nodes.len() < end {
                    self.current_step =
                        TraversalValue::NodeArray(nodes[start..nodes.len() - 1].to_vec());
                } else {
                    self.current_step = TraversalValue::NodeArray(nodes[start..end].to_vec());
                }
            }
            TraversalValue::EdgeArray(edges) => {
                if edges.len() == 0 {
                    self.current_step = TraversalValue::Empty;
                } else if edges.len() < end {
                    self.current_step =
                        TraversalValue::EdgeArray(edges[start..edges.len() - 1].to_vec());
                } else {
                    self.current_step = TraversalValue::EdgeArray(edges[start..end].to_vec());
                }
            }
            TraversalValue::Empty => {}
            _ => panic!("Invalid traversal step for range {:?}", &self.current_step),
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

    fn map_nodes<F>(&mut self, map_fn: F) -> &mut Self
    where
        F: Fn(&Node) -> Result<Node, GraphError>,
    {
        if let TraversalValue::NodeArray(nodes) = &mut self.current_step {
            let new_nodes = nodes.iter().map(|node| map_fn(node).unwrap()).collect();
            self.current_step = TraversalValue::NodeArray(new_nodes);
        }
        self
    }

    fn map_edges<F>(&mut self, map_fn: F) -> &mut Self
    where
        F: Fn(&Edge) -> Result<Edge, GraphError>,
    {
        if let TraversalValue::EdgeArray(edges) = &mut self.current_step {
            let new_edges = edges.iter().map(|edge| map_fn(edge).unwrap()).collect();
            self.current_step = TraversalValue::EdgeArray(new_edges);
        }
        self
    }

    fn for_each_node<F>(&mut self, map_fn: F) -> &mut Self
    where
        F: Fn(&Node) -> Result<(), GraphError>,
    {
        if let TraversalValue::NodeArray(nodes) = &self.current_step {
            for node in nodes {
                map_fn(node).unwrap();
            }
        }
        self
    }

    fn for_each_edge<F>(&mut self, map_fn: F) -> &mut Self
    where
        F: Fn(&Edge) -> Result<(), GraphError>,
    {
        if let TraversalValue::EdgeArray(edges) = &self.current_step {
            for edge in edges {
                map_fn(edge).unwrap();
            }
        }
        self
    }
}

impl<'a> TraversalSearchMethods for TraversalBuilder<'a> {
    fn shortest_path_between(&mut self, from_id: &str, to_id: &str) -> &mut Self {
        let s = Arc::clone(&self.storage);
        let paths = {
            match s.shortest_path(self.rtxn.as_ref().unwrap(), from_id, to_id) {
                Ok(paths) => paths,
                Err(err) => {
                    // self.store_error(err);
                    (vec![], vec![])
                }
            }
        };
        let mut v: Vec<(Vec<Node>, Vec<Edge>)> = Vec::with_capacity(1);
        v.push(paths);
        let new_current = TraversalValue::Paths(v);

        self
    }

    fn shortest_path_to(&mut self, to_id: &str) -> &mut Self {
        let mut paths = Vec::with_capacity(24);
        let nodes = match &self.current_step {
            TraversalValue::NodeArray(nodes) => nodes.clone(),
            _ => {
                println!(
                    "Invalid traversal step for shortest_path_to {:?}",
                    &self.current_step
                );
                unreachable!();
            }
        };
        for node in nodes {
            match self
                .storage
                .shortest_path(self.rtxn.as_ref().unwrap(), &node.id, to_id)
            {
                Ok(path) => paths.push(path),
                Err(e) => self.store_error(e),
            }
        }
        self.current_step = TraversalValue::Paths(paths);
        self
    }

    fn shortest_path_from(&mut self, from_id: &str) -> &mut Self {
        let mut paths = Vec::with_capacity(24);
        let nodes = match &self.current_step {
            TraversalValue::NodeArray(nodes) => nodes.clone(),
            _ => unreachable!(),
        };
        for node in nodes {
            match self
                .storage
                .shortest_path(self.rtxn.as_ref().unwrap(), from_id, &node.id)
            {
                Ok(path) => paths.push(path),
                Err(e) => self.store_error(e),
            }
        }
        self.current_step = TraversalValue::Paths(paths);
        self
    }
}

impl<'a> TraversalBuilderMethods for TraversalBuilder<'a> {
    fn result(self) -> Result<TraversalValue, GraphError> {
        if let Some(err) = self.error {
            return Err(err);
        }
        if let Some(txn) = self.wtxn {
            txn.commit().map_err(GraphError::from)?;
        }
        Ok(self.current_step)
    }

    fn execute(self) -> Result<(), GraphError> {
        if let Some(err) = self.error {
            return Err(err);
        }
        if let Some(txn) = self.wtxn {
            txn.commit().map_err(GraphError::from)?;
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{TraversalBuilder, TraversalMethods, TraversalSteps};
    use crate::{
        graph_core::traversal_steps::{SourceTraversalSteps, TraversalBuilderMethods},
        props,
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    };
    use protocol::{
        filterable::Filterable, traversal_value::TraversalValue, value::Value, Edge, Node,
    };
    use rayon::vec;
    use tempfile::TempDir;

    fn setup_test_db() -> (Arc<HelixGraphStorage>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();
        let storage = HelixGraphStorage::new(db_path).unwrap();
        (Arc::new(storage), temp_dir)
    }

    #[test]
    fn test_v() {
        let (storage, _temp_dir) = setup_test_db();
        let mut txn = storage.env.write_txn().unwrap();

        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let thing = storage.create_node(&mut txn, "thing", props!()).unwrap();
        txn.commit().unwrap();

        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
            _ => panic!("Expected NodeArray value {:?}", &traversal.current_step),
        }
    }

    #[test]
    fn test_e() {
        let (storage, _temp_dir) = setup_test_db();
        let mut txn = storage.env.write_txn().unwrap();

        // Graph Structure:
        // (person1)-[knows]->(person2)
        //         \-[likes]->(person3)
        // (person2)-[follows]->(person3)

        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person3 = storage.create_node(&mut txn, "person", props!()).unwrap();

        let knows_edge = storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();
        let likes_edge = storage
            .create_edge(&mut txn, "likes", &person1.id, &person3.id, props!())
            .unwrap();
        let follows_edge = storage
            .create_edge(&mut txn, "follows", &person2.id, &person3.id, props!())
            .unwrap();

        txn.commit().unwrap();

        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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

        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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

        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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

        let mut txn = storage.env.write_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty,  None,Some(txn));

        traversal.add_v("person", props! {});
        let result = traversal.result().unwrap();
        // Check that the current step contains a single node
        match &result {
            TraversalValue::NodeArray(node) => {
                assert_eq!(node.first().unwrap().label, "person");
            }
            _ => panic!("Expected SingleNode value"),
        }
    }

    #[test]
    fn test_add_e() {
        let (storage, _temp_dir) = setup_test_db();
        let mut txn = storage.env.write_txn().unwrap();

        let node1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let node2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        txn.commit().unwrap();
        let mut txn = storage.env.write_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, None, Some(txn));
        traversal.add_e("knows", &node1.id, &node2.id, props!());
        let result = traversal.result().unwrap();
        // Check that the current step contains a single edge
        match &result {
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create graph: (person1)-[knows]->(person2)-[knows]->(person3)
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person3 = storage.create_node(&mut txn, "person", props!()).unwrap();

        storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge(&mut txn, "knows", &person2.id, &person3.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person1.clone()),
            Some(txn),
            None,
        );
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create graph: (person1)-[knows]->(person2)
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        let edge = storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person1.clone()),
            Some(txn),
            None,
        );
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create graph: (person1)-[knows]->(person2)
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();
        txn.commit().unwrap();

        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person2.clone()),
            Some(txn),
            None,
        );
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create test graph: (person1)-[knows]->(person2)
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        let edge = storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person2.clone()),
            Some(txn),
            None,
        );
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
        let mut txn = storage.env.write_txn().unwrap();

        let node1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let node2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let edge = storage
            .create_edge(&mut txn, "knows", &node1.id, &node2.id, props!())
            .unwrap();
        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
        traversal.current_step = TraversalValue::from(edge);

        assert!(traversal.check_is_valid_node_traversal("test").is_err());

        traversal.current_step = TraversalValue::from(node1);
        assert!(traversal.check_is_valid_edge_traversal("test").is_err());
    }

    #[test]
    fn test_complex_traversal() {
        let (storage, _temp_dir) = setup_test_db();
        let mut txn = storage.env.write_txn().unwrap();

        // Graph structure:
        // (person1)-[knows]->(person2)-[likes]->(person3)
        //     ^                                     |
        //     |                                     |
        //     +-------<------[follows]------<-------+

        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person3 = storage.create_node(&mut txn, "person", props!()).unwrap();

        storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge(&mut txn, "likes", &person2.id, &person3.id, props!())
            .unwrap();
        storage
            .create_edge(&mut txn, "follows", &person3.id, &person1.id, props!())
            .unwrap();

        txn.commit().unwrap();

        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person1.clone()),
            Some(txn),
            None,
        );
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
        let mut txn = storage.env.write_txn().unwrap();
        let person = storage.create_node(&mut txn, "person", props!()).unwrap();
        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person),
            Some(txn),
            None,
        );
        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 1);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_count_node_array() {
        let (storage, _temp_dir) = setup_test_db();
        let mut txn = storage.env.write_txn().unwrap();
        let _ = storage.create_node(&mut txn, "person", props!()).unwrap();
        let _ = storage.create_node(&mut txn, "person", props!()).unwrap();
        let _ = storage.create_node(&mut txn, "person", props!()).unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create a graph with multiple paths
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person3 = storage.create_node(&mut txn, "person", props!()).unwrap();

        storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge(&mut txn, "knows", &person1.id, &person3.id, props!())
            .unwrap();
        txn.commit().unwrap();
        println!(
            "person1: {:?},\nperson2: {:?},\nperson3: {:?}",
            person1, person2, person3
        );

        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person1.clone()),
            Some(txn),
            None,
        );
        traversal.out("knows"); // Should have 2 nodes (person2 and person3)

        println!("Traversal: {:?}", traversal.current_step);

        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 2);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_range_subset() {
        let (storage, _temp_dir) = setup_test_db();
        let mut txn = storage.env.write_txn().unwrap();

        // Create multiple nodes
        let _: Vec<Node> = (0..5)
            .map(|_| storage.create_node(&mut txn, "person", props!()).unwrap())
            .collect();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create graph: (p1)-[knows]->(p2)-[knows]->(p3)-[knows]->(p4)-[knows]->(p5)
        let nodes: Vec<Node> = (0..5)
            .map(|i| {
                storage
                    .create_node(&mut txn, "person", props! { "name" => i })
                    .unwrap()
            })
            .collect();

        // Create edges connecting nodes sequentially
        for i in 0..4 {
            storage
                .create_edge(&mut txn, "knows", &nodes[i].id, &nodes[i + 1].id, props!())
                .unwrap();
        }

        storage
            .create_edge(&mut txn, "knows", &nodes[4].id, &nodes[0].id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();
        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();
        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
        if let TraversalValue::Count(count) = &traversal.count().current_step {
            assert_eq!(count.value(), 0);
        } else {
            panic!("Expected Count value");
        }
    }

    #[test]
    fn test_v_from_id() {
        let (storage, _temp_dir) = setup_test_db();
        let mut txn = storage.env.write_txn().unwrap();

        // Create a test node
        let person = storage.create_node(&mut txn, "person", props!()).unwrap();
        let node_id = person.id.clone();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create test graph: (person1)-[knows]->(person2)
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create test graph and edge
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let edge = storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();
        let edge_id = edge.id.clone();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
        traversal.v_from_id("nonexistent_id");
        let result = traversal.result();
        assert!(result.is_err());

        if let Err(e) = result {
            matches!(e, GraphError::NodeNotFound);
        }
    }

    #[test]
    fn test_e_from_id_nonexistent() {
        let (storage, _temp_dir) = setup_test_db();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
        traversal.e_from_id("nonexistent_id");
        let result = traversal.result();
        assert!(result.is_err());

        if let Err(e) = result {
            matches!(e, GraphError::EdgeNotFound);
        }
    }

    #[test]
    fn test_v_from_id_chain_operations() {
        let (storage, _temp_dir) = setup_test_db();
        let mut txn = storage.env.write_txn().unwrap();

        // Create test graph: (person1)-[knows]->(person2)-[likes]->(person3)
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person3 = storage.create_node(&mut txn, "person", props!()).unwrap();

        storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge(&mut txn, "likes", &person2.id, &person3.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create test graph and edges
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let edge = storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        // Create nodes with different properties
        let _ = storage
            .create_node(&mut txn, "person", props! { "age" => 25 })
            .unwrap();
        let _ = storage
            .create_node(&mut txn, "person", props! { "age" => 30 })
            .unwrap();
        let person3 = storage
            .create_node(&mut txn, "person", props! { "age" => 35 })
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        let _ = storage
            .create_node(&mut txn, "person", props! { "name" => "Alice" })
            .unwrap();
        let _ = storage
            .create_node(&mut txn, "person", props! { "name" => "Bob" })
            .unwrap();

        fn has_name(val: &Node) -> Result<bool, GraphError> {
            return Ok(val.check_property("name").is_some());
        }

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        let _ = storage
            .create_node(&mut txn, "person", props! { "age" => 25 })
            .unwrap();
        let person2 = storage
            .create_node(&mut txn, "person", props! { "age" => 30 })
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

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        let _ = storage
            .create_edge(
                &mut txn,
                "knows",
                &person1.id,
                &person2.id,
                props! { "since" => 2020 },
            )
            .unwrap();
        let edge2 = storage
            .create_edge(
                &mut txn,
                "knows",
                &person2.id,
                &person1.id,
                props! { "since" => 2022 },
            )
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        let _ = storage
            .create_node(&mut txn, "person", props! { "age" => 25 })
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        let _ = storage
            .create_node(
                &mut txn,
                "person",
                props! { "age" => 25, "name" => "Alice" },
            )
            .unwrap();
        let person2 = storage
            .create_node(&mut txn, "person", props! { "age" => 30, "name" => "Bob" })
            .unwrap();
        let _ = storage
            .create_node(&mut txn, "person", props! { "age" => 35 })
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        let edge = storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        let edge = storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
        let mut txn = storage.env.write_txn().unwrap();

        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person3 = storage.create_node(&mut txn, "person", props!()).unwrap();

        storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();
        storage
            .create_edge(&mut txn, "knows", &person2.id, &person3.id, props!())
            .unwrap();
        storage
            .create_edge(&mut txn, "knows", &person3.id, &person2.id, props!())
            .unwrap();
        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person2.clone()),
            Some(txn),
            None,
        );
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
        let mut txn = storage.env.write_txn().unwrap();
        let db = Arc::clone(&storage);
        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        let edge1 = storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();
        let edge2 = storage
            .create_edge(&mut txn, "likes", &person2.id, &person1.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person2.clone()),
            Some(txn),
            None,
        );
        traversal.both_e("knows");

        match &traversal.current_step {
            TraversalValue::EdgeArray(edges) => {
                assert_eq!(edges.len(), 1);
                assert_eq!(edges[0].id, edge1.id);
            }
            _ => panic!("Expected EdgeArray value"),
        }
        traversal.execute().unwrap();

        let txn = storage.env.read_txn().unwrap();
        let mut traversal = TraversalBuilder::new(
            Arc::clone(&storage),
            TraversalValue::from(person2.clone()),
            Some(txn),
            None,
        );
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
        let mut txn = storage.env.write_txn().unwrap();

        let person1 = storage.create_node(&mut txn, "person", props!()).unwrap();
        let person2 = storage.create_node(&mut txn, "person", props!()).unwrap();

        let edge = storage
            .create_edge(&mut txn, "knows", &person1.id, &person2.id, props!())
            .unwrap();

        txn.commit().unwrap();
        let txn = storage.env.read_txn().unwrap();
        let mut traversal =
            TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty, Some(txn), None);
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
