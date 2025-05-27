// use core::fmt;
// use std::ops::Deref;

// use serde::{
//     de::{DeserializeSeed, VariantAccess, Visitor},
//     ser::Error,
//     Deserializer, Serializer,
// };
// use sonic_rs::{Deserialize, Serialize, Value};

// pub enum ValidDate {
//     Date(chrono::DateTime<chrono::Utc>),


// #[derive(Debug, Copy, Clone, Eq, PartialEq)]
// #[repr(transparent)]

// pub struct Date<'a, K>(&'a K)
// where
//     K: Into<Value> + Deserialize<'a>;

// impl<'a, K> Date<'a, K>
// where
//     K: Into<Value> + Deserialize<'a>,
// {
//     pub fn inner(&self) -> &K {
//         self.0
//     }
// }

