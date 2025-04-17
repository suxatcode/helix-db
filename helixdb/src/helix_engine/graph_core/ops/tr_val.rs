use crate::{
    helix_engine::vector_core::vector::HVector,
    protocol::{
        count::Count, filterable::Filterable, items::{Edge, Node}
    },
};

#[derive(Debug, Clone)]
pub enum TraversalVal {
    Node(Node),
    Edge(Edge),
    Vector(HVector),
    Count(Count),
    Path((Vec<Node>, Vec<Edge>)),
    Empty
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