use super::source_steps;

pub struct Traversal {
    pub source_step: source_steps::SourceStep,
    pub steps: Vec<Step>,
}

use core::fmt;
use std::fmt::Display;

use super::{
    generator_types::{BoExp, GeneratedValue},
    types::GenRef,
};

pub enum Step {
    // graph steps
    Out(Out),
    In(In),
    OutE(OutE),
    InE(InE),
    FromN,
    ToN,

    // utils
    Count,
    Where(Where),
    Range(Range),
    OrderBy(OrderBy),
    Dedup,

    // bool ops
    Gt(GeneratedValue),
    Gte(GeneratedValue),
    Lt(GeneratedValue),
    Lte(GeneratedValue),
    Eq(GeneratedValue),
    Neq(GeneratedValue),
    Contains(GeneratedValue), // TODO: Implement

    EOF,
}

pub struct Out {
    pub label: String,
}

pub struct In {
    pub label: String,
}

pub struct OutE {
    pub label: String,
}

pub struct InE {
    pub label: String,
}

pub struct Where {
    pub expr: BoExp,
}

pub struct Range {
    pub start: u64,
    pub end: u64,
}

pub struct OrderBy {
    pub property: String,
    pub order: Order,
}
// TODO: probably move to protocol
pub enum Order {
    Asc,
    Desc,
}

impl Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Order::Asc => write!(f, "Asc"),
            Order::Desc => write!(f, "Desc"),
        }
    }
}
