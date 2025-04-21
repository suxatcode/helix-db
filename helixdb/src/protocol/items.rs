use super::count::Count;
use super::traversal_value::TraversalValue;
use super::value::{properties_format, Value};
use serde::{
    de::{DeserializeSeed, VariantAccess, Visitor},
    Deserializer, Serializer,
};
use sonic_rs::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, hash::Hash};

/// A node in the graph containing an ID, label, and property map.
/// Properties are serialised without enum variant names in JSON format.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct Node {
    pub id: u128,
    pub label: String,
    #[serde(with = "properties_format")]
    pub properties: HashMap<String, Value>,
}

impl Node {
    pub const NUM_PROPERTIES: usize = 2;
    pub fn new(label: &str, properties: Vec<(String, Value)>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().as_u128(),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
        }
    }
    pub fn new_with_id(label: &str, properties: Vec<(String, Value)>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().as_u128(),
            label: label.to_string(),
            properties: HashMap::from_iter(properties),
        }
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ id: {}, label: {}, properties: {:?} }}",
            self.id, self.label, self.properties
        )
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ id: {}, label: {}, properties: {:?} }}",
            self.id, self.label, self.properties
        )
    }
}

/// An edge in the graph connecting two nodes with an ID, label, and property map.
/// Properties are serialised without enum variant names in JSON format.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct Edge {
    pub id: u128, // TODO: change to uuid::Uuid and implement SERDE manually
    pub label: String,
    pub from_node: u128,
    pub to_node: u128,
    #[serde(with = "properties_format")]
    pub properties: HashMap<String, Value>,
}

impl Edge {
    pub const NUM_PROPERTIES: usize = 4;
    pub fn new(label: &str, properties: Vec<(String, Value)>) -> Self {
        Self { id: uuid::Uuid::new_v4().as_u128(), label: label.to_string(), from_node: 0, to_node: 0, properties: HashMap::from_iter(properties) }
    }

    pub fn new_with_id(label: &str, properties: Vec<(String, Value)>) -> Self {
        Self { id: uuid::Uuid::new_v4().as_u128(), label: label.to_string(), from_node: 0, to_node: 0, properties: HashMap::from_iter(properties) }
    }
}

impl std::fmt::Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ id: {}, label: {}, from_node: {}, to_node: {}, properties: {:?} }}",
            self.id, self.label, self.from_node, self.to_node, self.properties
        )
    }
}

impl std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ id: {}, label: {}, from_node: {}, to_node: {}, properties: {:?} }}",
            self.id, self.label, self.from_node, self.to_node, self.properties
        )
    }
}
