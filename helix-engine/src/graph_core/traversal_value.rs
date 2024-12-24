use std::{iter::once, vec::IntoIter};

use protocol::{Edge, Node, Value};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]

pub enum TraversalValue {
    Empty,
    SingleNode(Node),
    SingleEdge(Edge),
    SingleValue(Value),
    NodeArray(Vec<Node>),
    EdgeArray(Vec<Edge>),
    ValueArray(Vec<Value>),
}

pub trait AsTraversalValue {
    fn as_traversal_value(&self) -> &TraversalValue;
}

impl AsTraversalValue for TraversalValue {
    fn as_traversal_value(&self) -> &TraversalValue {
        self
    }
}

enum IterState {
    Empty,
    Single(TraversalValue),
    Nodes(std::vec::IntoIter<Node>),
    Edges(std::vec::IntoIter<Edge>),
    Values(std::vec::IntoIter<Value>),
}

pub struct TraversalValueIterator {
    state: IterState,
}

impl TraversalValue {
    pub fn iter(&self) -> TraversalValueIterator {
        match self {
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
