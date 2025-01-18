use count::Count;
use serde::{
    ser::{SerializeMap, SerializeSeq}, Deserialize, Deserializer, Serialize
};
use std::collections::HashMap;

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

#[derive(Clone, Serialize, Deserialize)]
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
#[derive(Serialize, PartialEq, Clone, Debug)]
#[serde(untagged)]
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

// impl std::fmt::Display for Value {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Value::String(s) => write!(f, "{}", s),
//             Value::Float(fl) => write!(f, "{}", fl),
//             Value::Integer(i) => write!(f, "{}", i),
//             Value::Boolean(b) => write!(f, "{}", b),
//             Value::Array(arr) => {
//                 write!(f, "[")?;
//                 for (i, v) in arr.iter().enumerate() {
//                     write!(f, "{}", v)?;
//                     if i < arr.len() - 1 {
//                         write!(f, ", ")?;
//                     }
//                 }
//                 write!(f, "]")
//             }
//             Value::Empty => write!(f, "Empty"),
//         }
//     }
// }

// impl std::fmt::Debug for Value {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Value::String(s) => write!(f, "{}", s),
//             Value::Float(fl) => write!(f, "{}", fl),
//             Value::Integer(i) => write!(f, "{}", i),
//             Value::Boolean(b) => write!(f, "{}", b),
//             Value::Array(arr) => {
//                 write!(f, "[")?;
//                 for (i, v) in arr.iter().enumerate() {
//                     write!(f, "{:?}", v)?;
//                     if i < arr.len() - 1 {
//                         write!(f, ", ")?;
//                     }
//                 }
//                 write!(f, "]")
//             }
//             Value::Empty => write!(f, "Empty"),
//         }
//     }
// }

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    Ok(Value::Integer(n.as_i64().unwrap() as i32))
                } else {
                    Ok(Value::Float(n.as_f64().unwrap()))
                }
            },
            serde_json::Value::String(s) => Ok(Value::String(s)),
            serde_json::Value::Bool(b) => Ok(Value::Boolean(b)),
            serde_json::Value::Array(arr) => {
                let mut values = Vec::new();
                for v in arr {
                    values.push(Value::deserialize(v).unwrap());
                }
                Ok(Value::Array(values))
            }
            _ => Ok(Value::Empty),

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
