use super::count::Count;
use serde::{
    de::{DeserializeSeed, VariantAccess, Visitor},
    Deserializer, Serializer,
};
use sonic_rs::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};
use super::value::{properties_format, Value};
use super::traversal_value::TraversalValue;


/// A return value enum that represents different possible outputs from graph operations.
/// Can contain traversal results, counts, boolean flags, or empty values.
#[derive(Deserialize, Debug, Clone)]
pub enum ReturnValue {
    TraversalValues(TraversalValue),
    Count(Count),
    Boolean(bool),
    Value(Value),
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
            ReturnValue::Value(value) => value.serialize(serializer),
            ReturnValue::Empty => serializer.serialize_none(),
        }
    }
}
