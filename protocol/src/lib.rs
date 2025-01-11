
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod request;
pub mod response;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReturnValue {
    NodeArray(Vec<Node>),
    Node(Node),
    EdgeArray(Vec<Edge>),
    Edge(Edge),
    ValueArray(Vec<Value>),
    Value(Value),
    Empty,
}

impl From<Vec<Node>> for ReturnValue {
    fn from(n: Vec<Node>) -> Self {
        ReturnValue::NodeArray(n)
    }
}
impl From<Node> for ReturnValue {
    fn from(n: Node) -> Self {
        ReturnValue::Node(n)
    }
}
impl From<Vec<Edge>> for ReturnValue {
    fn from(e: Vec<Edge>) -> Self {
        ReturnValue::EdgeArray(e)
    }
}
impl From<Edge> for ReturnValue {
    fn from(e: Edge) -> Self {
        ReturnValue::Edge(e)
    }
}
impl From<Vec<Value>> for ReturnValue {
    fn from(v: Vec<Value>) -> Self {
        ReturnValue::ValueArray(v)
    }
}
impl From<Value> for ReturnValue {
    fn from(v: Value) -> Self {
        ReturnValue::Value(v)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub id: String,
    pub label: String,
    pub properties: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Edge {
    pub id: String,
    pub label: String,
    pub from_node: String,
    pub to_node: String,
    pub properties: HashMap<String, Value>,
}

// TODO: implement into for Uint handling 
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
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
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
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
