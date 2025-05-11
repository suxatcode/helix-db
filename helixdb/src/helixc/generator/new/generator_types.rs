use core::fmt;
use std::{collections::HashMap, fmt::Display};

use crate::protocol::value::Value;

use super::traversal_steps::Traversal;

pub struct Source {}

pub struct NodeSchema {
    pub name: String,
    pub properties: Vec<(String, GeneratedType)>,
}

pub struct EdgeSchema {
    pub name: String,
    pub from: String,
    pub to: String,
    pub properties: Vec<(String, GeneratedType)>,
}

pub struct VectorSchema {
    pub name: String,
    pub properties: Vec<(String, GeneratedType)>,
}

pub struct Query {
    pub name: String,
    pub statements: Vec<Statement>,
    pub parameters: Vec<Parameter>, // iterate through and print each one
    pub sub_parameters: Vec<Vec<Parameter>>, // iterate through and construct each one
}

pub struct Parameter {
    pub name: String,
    pub type_name: GeneratedType,
}

pub enum Statement {
    Assignment(Assignment),
    Drop(Drop),
    Traversal(Traversal),
    ForEach(ForEach),
}

pub struct Assignment {
    // TODO: IMPLEMENT
}

pub struct ForEach {
    // TODO: IMPLEMENT
}

pub struct Drop {
    pub expression: Traversal,
}

/// Boolean expression is used for a traversal or set of traversals wrapped in AND/OR
/// that resolve to a boolean value
pub enum BoExp {
    And(Vec<Traversal>),
    Or(Vec<Traversal>),
    // Not(Traversal),
    // Eq(Traversal, Traversal),
    Expr(Traversal),
}
impl Display for BoExp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoExp::And(traversals) => {
                let tr = traversals
                    .iter()
                    .map(|s| format!("{}", s))
                    .collect::<Vec<_>>();
                write!(f, "{}", tr.join(" && "))
            }
            BoExp::Or(traversals) => {
                let tr = traversals
                    .iter()
                    .map(|s| format!("{}", s))
                    .collect::<Vec<_>>();
                write!(f, "{}", tr.join(" || "))
            }
            BoExp::Expr(traversal) => write!(f, "{}", traversal),
        }
    }
}

pub enum GeneratedValue {
    // needed?
    Literal(String),
}

impl Display for GeneratedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedValue::Literal(value) => write!(f, "{}", value),
        }
    }
}

pub enum GeneratedType {
    RustType(RustType),
    Vec(Box<GeneratedType>),
    Variable(String),
}

impl Display for GeneratedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedType::RustType(t) => write!(f, "{}", t),
            GeneratedType::Vec(t) => write!(f, "Vec<{}>", t),
            GeneratedType::Variable(v) => write!(f, "{}", v),
        }
    }
}

pub enum RustType {
    String,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Bool,
}
impl Display for RustType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RustType::String => write!(f, "String"),
            RustType::I8 => write!(f, "i8"),
            RustType::I16 => write!(f, "i16"),
            RustType::I32 => write!(f, "i32"),
            RustType::I64 => write!(f, "i64"),
            RustType::U8 => write!(f, "u8"),
            RustType::U16 => write!(f, "u16"),
            RustType::U32 => write!(f, "u32"),
            RustType::U64 => write!(f, "u64"),
            RustType::U128 => write!(f, "u128"),
            RustType::F32 => write!(f, "f32"),
            RustType::F64 => write!(f, "f64"),
            RustType::Bool => write!(f, "bool"),
        }
    }
}
