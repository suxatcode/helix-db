use super::traversal_value::TraversalValue;
use super::value::{properties_format, Value};
use super::{count::Count, return_values::ReturnValue};
use serde::{
    de::{DeserializeSeed, VariantAccess, Visitor},
    Deserializer, Serializer,
};
use sonic_rs::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

#[derive(Deserialize, Debug, Clone)]
pub struct Remapping {
    original_name: String,
    return_value: ReturnValue,
}

impl Serialize for Remapping {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        self.return_value.serialize(serializer)
    }
}

impl Remapping {
    pub fn new(original_name: String, return_value: ReturnValue) -> Self {
        Self { original_name, return_value }
    }
}
