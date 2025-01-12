
use count::Count;
use serde::{ser::{SerializeMap, SerializeSeq}, Deserialize, Serialize};
use std::collections::HashMap;

pub mod request;
pub mod response;
pub mod traversal_value;
pub mod count;

#[derive(Deserialize, Debug, Clone)]
pub enum ReturnValue {
    TraversalValues(Vec<traversal_value::TraversalValue>),
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
            ReturnValue::TraversalValues(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    seq.serialize_element(value)?;
                }
                seq.end()
            }
            ReturnValue::Count(count) => count.serialize(serializer),
            ReturnValue::Boolean(b) => serializer.serialize_bool(*b),
            ReturnValue::Empty => serializer.serialize_none(),
        }
    }
}



#[derive(Deserialize, Clone)]
pub struct Node {
    pub id: String,
    pub label: String,
    pub properties: HashMap<String, Value>,
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ id: {}, label: {}, properties: {:?} }}", self.id, self.label, self.properties)
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ id: {}, label: {}, properties: {:?} }}", self.id, self.label, self.properties)
    }
}

impl Serialize for Node {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut node = serializer.serialize_map(Some(3))?;
        node.serialize_entry("id", &self.id)?;
        node.serialize_entry("label", &self.label)?;
        node.serialize_entry("properties", &self.properties)?;
        node.end()
    }
}

#[derive(Deserialize, Clone)]
pub struct Edge {
    pub id: String,
    pub label: String,
    pub from_node: String,
    pub to_node: String,
    pub properties: HashMap<String, Value>,
}

impl std::fmt::Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ id: {}, label: {}, from_node: {}, to_node: {}, properties: {:?} }}", self.id, self.label, self.from_node, self.to_node, self.properties)
    }
}

impl std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ id: {}, label: {}, from_node: {}, to_node: {}, properties: {:?} }}", self.id, self.label, self.from_node, self.to_node, self.properties)
    }
}

impl Serialize for Edge {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut edge = serializer.serialize_map(Some(5))?;
        edge.serialize_entry("id", &self.id)?;
        edge.serialize_entry("label", &self.label)?;
        edge.serialize_entry("from_node", &self.from_node)?;
        edge.serialize_entry("to_node", &self.to_node)?;
        edge.serialize_entry("properties", &self.properties)?;
        edge.end()
    }
}

// TODO: implement into for Uint handling 
#[derive(Deserialize, PartialEq, Clone)]
pub enum Value {
    String(String),
    Float(f64),
    Integer(i32),
    Boolean(bool),
    Array(Vec<Value>),
    Empty,
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

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    write!(f, "{}", v)?;
                    if i < arr.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "]")
            }
            Value::Empty => write!(f, "Empty"),
        }
    }
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    write!(f, "{:?}", v)?;
                    if i < arr.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "]")
            }
            Value::Empty => write!(f, "Empty"),
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match self {
            Value::String(s) => serializer.serialize_str(s),
            Value::Float(fl) => serializer.serialize_f64(*fl),
            Value::Integer(i) => serializer.serialize_i32(*i),
            Value::Boolean(b) => serializer.serialize_bool(*b),
            Value::Array(arr) => {
                let mut seq = serializer.serialize_seq(Some(arr.len()))?;
                for v in arr {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Value::Empty => serializer.serialize_none(),
        }
    }
}

// impl Deserialize for Value {

// }