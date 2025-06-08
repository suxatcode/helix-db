use std::fmt::Display;

use super::super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        types::GraphError,
    },
    helix_storage::Storage,
    protocol::value::Value,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EdgeType {
    #[serde(rename = "vec")]
    Vec,
    #[serde(rename = "node")]
    Node,
}
impl Display for EdgeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EdgeType::Vec => write!(f, "EdgeType::Vec"),
            EdgeType::Node => write!(f, "EdgeType::Node"),
        }
    }
}
pub struct AddE {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for AddE {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait AddEAdapter<'a, 'b, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn add_e(
        self,
        label: &'a str,
        properties: Option<Vec<(String, Value)>>,
        from_node: u128,
        to_node: u128,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, 'b, I, S> AddEAdapter<'a, 'b, S> for RwTraversalIterator<'a, 'b, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    #[inline(always)]
    fn add_e(
        mut self,
        label: &'a str,
        properties: Option<Vec<(String, Value)>>,
        from_node: u128,
        to_node: u128,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let result = self
            .storage
            .add_edge(self.txn, label, properties, from_node, to_node)
            .map(TraversalVal::Edge);

        RwTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
