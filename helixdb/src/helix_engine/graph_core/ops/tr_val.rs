use std::hash::Hash;

use crate::{
    helix_engine::{types::GraphError, vector_core::vector::HVector},
    protocol::{
        count::Count,
        filterable::Filterable,
        items::{Edge, Node},
        value::Value,
    },
};

#[derive(Debug, Clone)]
pub enum TraversalVal {
    Node(Node),
    Edge(Edge),
    Vector(HVector),
    Count(Count),
    Path((Vec<Node>, Vec<Edge>)),
    Empty,
}

impl Hash for TraversalVal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            TraversalVal::Node(node) => node.id.hash(state),
            TraversalVal::Edge(edge) => edge.id.hash(state),
            TraversalVal::Vector(vector) => vector.id().hash(state),
            TraversalVal::Empty => state.write_u8(0),
            _ => state.write_u8(0),
        }
    }
}

impl Eq for TraversalVal {}
impl PartialEq for TraversalVal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TraversalVal::Node(node1), TraversalVal::Node(node2)) => node1.id == node2.id,
            (TraversalVal::Edge(edge1), TraversalVal::Edge(edge2)) => edge1.id == edge2.id,
            (TraversalVal::Vector(vector1), TraversalVal::Vector(vector2)) => {
                vector1.id() == vector2.id()
            }
            (TraversalVal::Empty, TraversalVal::Empty) => true,
            _ => false,
        }
    }
}

pub trait Traversable {
    fn id(&self) -> u128;
    fn label(&self) -> String;
    fn check_property(&self, prop: &str) -> Option<&Value>;
}

impl Traversable for TraversalVal {
    fn id(&self) -> u128 {
        match self {
            TraversalVal::Node(node) => node.id,
            TraversalVal::Edge(edge) => edge.id,
            TraversalVal::Vector(vector) => *vector.id(),
            _ => panic!("Invalid traversal value"),
        }
    }

    fn label(&self) -> String {
        match self {
            TraversalVal::Node(node) => node.label.clone(),
            TraversalVal::Edge(edge) => edge.label.clone(),
            _ => panic!("Invalid traversal value"),
        }
    }

    fn check_property(&self, prop: &str) -> Option<&Value> {
        match self {
            TraversalVal::Node(node) => node.check_property(prop),
            TraversalVal::Edge(edge) => edge.check_property(prop),
            TraversalVal::Vector(vector) => vector.check_property(prop),
            _ => None,
        }
    }
}
