use crate::{count::Count, Edge, Filterable, Node, Value};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum TraversalValue {
    Empty,
    Count(Count),
    NodeArray(Vec<Node>),
    EdgeArray(Vec<Edge>),
    ValueArray(Vec<(String, Value)>),
}

impl FromIterator<TraversalValue> for TraversalValue {
    fn from_iter<T: IntoIterator<Item = TraversalValue>>(iter: T) -> Self {
        let mut nodes = Vec::with_capacity(10);
        let mut edges = Vec::with_capacity(10);
        let mut values = Vec::with_capacity(10);

        for value in iter {
            match value {
                TraversalValue::Count(count) => return TraversalValue::Count(count),
                TraversalValue::NodeArray(mut node_vec) => nodes.append(&mut node_vec),
                TraversalValue::EdgeArray(mut edge_vec) => edges.append(&mut edge_vec),
                TraversalValue::ValueArray(mut value_vec) => values.append(&mut value_vec),
                TraversalValue::Empty => (),
            }
        }

        if !nodes.is_empty() {
            TraversalValue::NodeArray(nodes)
        } else if !edges.is_empty() {
            TraversalValue::EdgeArray(edges)
        } else if !values.is_empty() {
            TraversalValue::ValueArray(values)
        } else {
            TraversalValue::Empty
        }
    }
}

impl From<Node> for TraversalValue {
    fn from(node: Node) -> Self {
        TraversalValue::NodeArray(vec![node])
    }
}

impl From<Edge> for TraversalValue {
    fn from(edge: Edge) -> Self {
        TraversalValue::EdgeArray(vec![edge])
    }
}

impl From<&Node> for TraversalValue {
    fn from(node: &Node) -> Self {
        TraversalValue::NodeArray(vec![node.clone()])
    }
}

impl From<&Edge> for TraversalValue {
    fn from(edge: &Edge) -> Self {
        TraversalValue::EdgeArray(vec![edge.clone()])
    }
}
impl From<(String, Value)> for TraversalValue {
    fn from(value: (String, Value)) -> Self {
        TraversalValue::ValueArray(vec![value])
    }
}

enum IterState {
    Empty,
    Single(TraversalValue),
    Nodes(std::vec::IntoIter<Node>),
    Edges(std::vec::IntoIter<Edge>),
    Values(std::vec::IntoIter<(String, Value)>),
}

pub struct TraversalValueIterator {
    state: IterState,
}

impl TraversalValue {
    pub fn iter(&self) -> TraversalValueIterator {
        match self {
            TraversalValue::Count(count) => TraversalValueIterator {
                state: IterState::Single(TraversalValue::Count(count.clone())),
            },
            TraversalValue::Empty => TraversalValueIterator {
                state: IterState::Empty,
            },
            TraversalValue::NodeArray(nodes) => TraversalValueIterator {
                state: IterState::Nodes(nodes.clone().into_iter()),
            },
            TraversalValue::EdgeArray(edges) => TraversalValueIterator {
                state: IterState::Edges(edges.clone().into_iter()),
            },
            TraversalValue::ValueArray(values) => TraversalValueIterator {
                state: IterState::Values(values.clone().into_iter()),
            },
        }
    }
}

impl std::fmt::Debug for TraversalValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraversalValue::Count(count) => write!(f, "Count: {:?}", count.value()),
            TraversalValue::Empty => write!(f, "[]"),
            TraversalValue::NodeArray(nodes) => nodes.fmt(f),
            TraversalValue::EdgeArray(edges) => edges.fmt(f),
            TraversalValue::ValueArray(values) => values.fmt(f),
        }
    }
}

impl Serialize for TraversalValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            TraversalValue::Empty => serializer.serialize_none(),
            TraversalValue::Count(count) => count.serialize(serializer),
            TraversalValue::NodeArray(nodes) => nodes.serialize(serializer),
            TraversalValue::EdgeArray(edges) => edges.serialize(serializer),
            TraversalValue::ValueArray(values) => values.serialize(serializer),
        }
    }
}
