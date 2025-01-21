use count::Count;
use serde::{
    de::{value::Error, DeserializeOwned}, ser::{SerializeMap, SerializeSeq}, Serializer
};
use sonic_rs::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap};

pub mod count;
pub mod request;
pub mod response;
pub mod traversal_value;

#[derive(Deserialize, Debug, Clone)]
pub enum ReturnValue {
    TraversalValues(traversal_value::TraversalValue),
    Count(Count),
    Boolean(bool),
    Empty,
}

impl  Serialize for ReturnValue  {
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

#[derive(Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub label: String,
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

#[derive(Serialize, Deserialize, Clone)]
pub struct Edge {
    pub id: String,
    pub label: String,
    pub from_node: String,
    pub to_node: String,
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

// TODO: implement into for Uint handling
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum Value {
    String(String),
    Float(f64),
    Integer(i32),
    Boolean(bool),
    Array(Vec<Value>),
    Empty
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.trim_matches('"').to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s.trim_matches('"').to_string())
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Integer(i)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::Array(v)
    }
}

impl From<Value> for String {
    fn from(v: Value) -> Self {
        match v {
            Value::String(s) => s,
            _ => panic!("Value is not a string"),
        }
    }
}

pub trait Filterable {
    fn check_property(&self, key: &str) -> Option<&Value>;
}

impl Filterable for Node {
    fn check_property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }
}

impl Filterable for Edge {
    fn check_property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }
}
