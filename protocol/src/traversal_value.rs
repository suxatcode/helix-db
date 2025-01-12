use crate::{count::Count, Edge, Node, Value};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone)]
pub enum TraversalValue {
    Empty,
    Count(Count),
    SingleNode(Node),
    SingleEdge(Edge),
    SingleValue((String, Value)),
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
                TraversalValue::SingleNode(node) => nodes.push(node),
                TraversalValue::SingleEdge(edge) => edges.push(edge),
                TraversalValue::SingleValue(value) => values.push(value),
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

enum IterState {
    Empty,
    Single(TraversalValue),
    Nodes(std::vec::IntoIter<Node>),
    Edges(std::vec::IntoIter<Edge>),
    Values(std::vec::IntoIter<(String,Value)>),
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
            TraversalValue::SingleNode(node) => TraversalValueIterator {
                state: IterState::Single(TraversalValue::SingleNode(node.clone())),
            },
            TraversalValue::SingleEdge(edge) => TraversalValueIterator {
                state: IterState::Single(TraversalValue::SingleEdge(edge.clone())),
            },
            TraversalValue::SingleValue(value) => TraversalValueIterator {
                state: IterState::Single(TraversalValue::SingleValue(value.clone())),
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

impl<'a> IntoIterator for &'a TraversalValue {
    type Item = TraversalValue;
    type IntoIter = TraversalValueIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Iterator for TraversalValueIterator {
    type Item = TraversalValue;

    /// consumes next value and replaces location with empty value to avoid dereference issues
    fn next(&mut self) -> Option<Self::Item> {
        match std::mem::replace(&mut self.state, IterState::Empty) {
            IterState::Empty => None,
            IterState::Single(value) => Some(value),
            IterState::Nodes(mut iter) => match iter.next() {
                Some(node) => {
                    self.state = IterState::Nodes(iter);
                    Some(TraversalValue::SingleNode(node))
                }
                None => None,
            },
            IterState::Edges(mut iter) => match iter.next() {
                Some(edge) => {
                    self.state = IterState::Edges(iter);
                    Some(TraversalValue::SingleEdge(edge))
                }
                None => None,
            },
            IterState::Values(mut iter) => match iter.next() {
                Some(value) => {
                    self.state = IterState::Values(iter);
                    Some(TraversalValue::SingleValue(value))
                }
                None => None,
            },
        }
    }
}

impl std::fmt::Debug for TraversalValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraversalValue::Count(count) => write!(f, "Count: {:?}", count.value()),
            TraversalValue::Empty => write!(f, "[]"),
            TraversalValue::SingleNode(node) => node.fmt(f),
            TraversalValue::SingleEdge(edge) => edge.fmt(f),
            TraversalValue::SingleValue(value) => value.fmt(f),
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
            TraversalValue::SingleNode(node) => node.serialize(serializer),
            TraversalValue::SingleEdge(edge) => edge.serialize(serializer),
            TraversalValue::SingleValue(value) => value.serialize(serializer),
            TraversalValue::NodeArray(nodes) => nodes.serialize(serializer),
            TraversalValue::EdgeArray(edges) => edges.serialize(serializer),
            TraversalValue::ValueArray(values) => values.serialize(serializer),
        }
    }
}