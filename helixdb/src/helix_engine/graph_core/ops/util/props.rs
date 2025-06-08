use crate::helix_engine::{
    graph_core::{
        ops::tr_val::TraversalVal,
        traversal_iter::{RoTraversalIterator, RwTraversalIterator},
    },
    types::GraphError,
};
use crate::helix_storage::Storage;

pub struct PropsIterator<'a, I> {
    iter: I,
    prop: &'a str,
}

// TODO: get rid of clones in return values
impl<'a, I> Iterator for PropsIterator<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(TraversalVal::Node(node))) => match node.properties {
                Some(prop) => {
                    let prop = prop.get(self.prop);
                    match prop {
                        Some(prop) => Some(Ok(TraversalVal::Value(prop.clone()))),
                        None => None,
                    }
                }
                None => None,
            },
            Some(Ok(TraversalVal::Edge(edge))) => match edge.properties {
                Some(prop) => {
                    let prop = prop.get(self.prop);
                    match prop {
                        Some(prop) => Some(Ok(TraversalVal::Value(prop.clone()))),
                        None => None,
                    }
                }
                None => None,
            },
            Some(Ok(TraversalVal::Vector(vec))) => match vec.properties {
                Some(prop) => {
                    let prop = prop.get(self.prop);
                    match prop {
                        Some(prop) => Some(Ok(TraversalVal::Value(prop.clone()))),
                        None => None,
                    }
                }
                None => None,
            },
            _ => None,
        }
    }
}
pub trait PropsAdapter<'a, I, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn check_property(
        self,
        prop: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}

impl<'a, I, S> PropsAdapter<'a, I, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    #[inline]
    fn check_property(
        self,
        prop: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        RoTraversalIterator {
            inner: PropsIterator {
                iter: self.inner,
                prop,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}

impl<'a, 'b, I, S> PropsAdapter<'a, I, S> for RwTraversalIterator<'a, 'b, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
    'b: 'a,
{
    #[inline]
    fn check_property(
        self,
        prop: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        RoTraversalIterator {
            inner: PropsIterator {
                iter: self.inner,
                prop,
            },
            storage: self.storage,
            txn: &*self.txn,
        }
    }
}
