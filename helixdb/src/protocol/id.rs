use core::fmt;

use serde::{
    de::{DeserializeSeed, VariantAccess, Visitor},
    ser::Error,
    Deserializer, Serializer,
};
use sonic_rs::{Deserialize, Serialize};
// pub type ID = String;
pub struct ID {
    id: String,
}

impl Serialize for ID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match uuid::Uuid::parse_str(&self.id) {
            Ok(uuid) => serializer.serialize_u128(uuid.as_u128()),
            Err(e) => Err(Error::custom(e)),
        }
    }
}

struct IDVisitor;

impl<'de> Visitor<'de> for IDVisitor {
    type Value = ID;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid UUID")
    }

    fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(ID {
            id: uuid::Uuid::from_u128(v).to_string(),
        })
    }
}
impl<'de> Deserialize<'de> for ID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u128(IDVisitor)
    }
}
