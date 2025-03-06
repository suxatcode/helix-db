use std::collections::HashMap;

use crate::protocol::{
    items::{Edge, Node},
    value::Value,
};

use super::return_values::ReturnValue;

/// Trait for types that can be filtered based on their properties.
/// Implemented by both Node and Edge types.
pub trait Filterable<'a> {
    fn check_property(&'a self, key: &str) -> Option<&'a Value>;

    fn find_property(
        &'a self,
        key: &str,
        secondary_properties: &'a HashMap<String, ReturnValue>,
        property: &'a mut ReturnValue,
    ) -> Option<&'a ReturnValue>;
}

impl<'a> Filterable<'a> for Node {
    #[inline(always)]
    fn check_property(&'a self, key: &str) -> Option<&'a Value> {
        self.properties.get(key)
    }

    #[inline(always)]
    fn find_property(
        &'a self,
        key: &str,
        secondary_properties: &'a HashMap<String, ReturnValue>,
        property: &'a mut ReturnValue,
    ) -> Option<&'a ReturnValue> {
        match self.properties.get(key) {
            Some(value) => {
                property.clone_from(&ReturnValue::Value(value.clone()));
                Some(property)
            }
            None => secondary_properties.get(key),
        }
    }
}

impl<'a> Filterable<'a> for Edge {
    #[inline(always)]
    fn check_property(&'a self, key: &str) -> Option<&'a Value> {
        self.properties.get(key)
    }

    #[inline(always)]
    fn find_property(
        &'a self,
        key: &str,
        secondary_properties: &'a HashMap<String, ReturnValue>,
        property: &'a mut ReturnValue,
    ) -> Option<&'a ReturnValue> {
        match self.properties.get(key) {
            Some(value) => {
                property.clone_from(&ReturnValue::Value(value.clone()));
                Some(property)
            }
            None => secondary_properties.get(key),
        }
    }
}
