use count::Count;
use serde::{
    de::{DeserializeSeed, VariantAccess, Visitor},
    Deserializer, Serializer,
};
use sonic_rs::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};
use value::{properties_format, Value};

pub mod count;
pub mod filterable;
pub mod request;
pub mod response;
pub mod traversal_value;
pub mod value;

/// A return value enum that represents different possible outputs from graph operations.
/// Can contain traversal results, counts, boolean flags, or empty values.
#[derive(Deserialize, Debug, Clone)]
pub enum ReturnValue {
    TraversalValues(traversal_value::TraversalValue),
    Count(Count),
    Boolean(bool),
    Empty,
}

impl Serialize for ReturnValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match self {
            ReturnValue::TraversalValues(values) => values.serialize(serializer),
            ReturnValue::Count(count) => count.serialize(serializer),
            ReturnValue::Boolean(b) => serializer.serialize_bool(*b),
            ReturnValue::Empty => serializer.serialize_none(),
        }
    }
}

/// A node in the graph containing an ID, label, and property map.
/// Properties are serialised without enum variant names in JSON format.
#[derive(Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub label: String,
    #[serde(with = "properties_format")]
    pub properties: HashMap<String, Value>,
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
#[derive(Serialize, Deserialize, Clone)]
pub struct Edge {
    pub id: String,
    pub label: String,
    pub from_node: String,
    pub to_node: String,
    #[serde(with = "properties_format")]
    pub properties: HashMap<String, Value>,
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
