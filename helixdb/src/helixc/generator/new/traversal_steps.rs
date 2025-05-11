use super::{
    generator_types::{BoExp, GeneratedValue},
    source_steps::SourceStep,
    types::GenRef,
};
use core::fmt;
use std::fmt::Display;

pub struct Traversal {
    pub source_step: SourceStep,
    pub steps: Vec<Step>,
}
impl Display for Traversal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source_step)?;
        for step in &self.steps {
            write!(f, "{}", step)?;
        }
        Ok(())
    }
}
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

    // property
    Property(GenRef<String>),

    EOF,
}
impl Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::Count => write!(f, "count()"),
            Step::Dedup => write!(f, "dedup()"),
            Step::FromN => write!(f, "from_n()"),
            Step::ToN => write!(f, "to_n()"),
            Step::Property(property) => write!(f, "check_property({})", property),
            _ => write!(f, "{}", self),
        }
    }
}
pub struct Out {
    pub label: GenRef<String>,
}
impl Display for Out {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "out({})", self.label)
    }
}

pub struct In {
    pub label: GenRef<String>,
}
impl Display for In {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "in({})", self.label)
    }
}

pub struct OutE {
    pub label: GenRef<String>,
}
impl Display for OutE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "out_e({})", self.label)
    }
}

pub struct InE {
    pub label: GenRef<String>,
}
impl Display for InE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "in_e({})", self.label)
    }
}

pub enum Where {
    Ref(WhereRef),
    Mut(WhereMut),
}

pub struct WhereRef {
    pub expr: BoExp,
}
impl Display for WhereRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "filter_ref(||val, txn|{{
                if let Ok(val) = val {{ 
                    {}
                }} else {{
                    Ok(false)
                }}
            }})",
            self.expr
        )
    }
}

pub struct WhereMut {
    pub expr: BoExp,
}
impl Display for WhereMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NOT IMPLEMENTED")
    }
}

pub struct Range {
    pub start: u64,
    pub end: u64,
}
impl Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "range({}, {})", self.start, self.end)
    }
}

pub struct OrderBy {
    pub property: String,
    pub order: Order,
}
impl Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "order_by({}, HelixOrder::{})", self.property, self.order)
    }
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
