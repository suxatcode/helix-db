use crate::protocol::{
    items::{Edge, Node},
    value::Value,
};

/// Trait for types that can be filtered based on their properties.
/// Implemented by both Node and Edge types.
pub trait Filterable {
    fn check_property(&self, key: &str) -> Option<&Value>;
}

impl Filterable for Node {
    #[inline(always)]
    fn check_property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }
}

impl Filterable for Edge {
    #[inline(always)]
    fn check_property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }
}
