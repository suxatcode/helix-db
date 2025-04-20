use std::hash::Hash;

use crate::{
    helix_engine::vector_core::vector::HVector,
    protocol::{
        count::Count,
        filterable::Filterable,
        items::{Edge, Node},
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
    fn id<'a>(&'a self) -> &'a str;
    fn label<'a>(&'a self) -> &'a str;
}

impl Traversable for TraversalVal {
    fn id<'a>(&'a self) -> &'a str {
        match self {
            TraversalVal::Node(node) => node.id.as_str(),
            TraversalVal::Edge(edge) => edge.id.as_str(),
            TraversalVal::Vector(vector) => vector.id(),
            _ => panic!("Invalid traversal value"),
        }
    }

    fn label<'a>(&'a self) -> &'a str {
        match self {
            TraversalVal::Node(node) => node.label.as_str(),
            TraversalVal::Edge(edge) => edge.label.as_str(),
            _ => panic!("Invalid traversal value"),
        }
    }
}
