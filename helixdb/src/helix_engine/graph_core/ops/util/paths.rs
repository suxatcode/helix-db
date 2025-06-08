use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        types::GraphError,
    },
    helix_storage::Storage,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum PathType<'a> {
    From(&'a u128),
    To(&'a u128),
}

pub struct ShortestPathIterator<'a, I, S: Storage + ?Sized> {
    iter: I,
    path_type: PathType<'a>,
    edge_label: Option<&'a str>,
    storage: Arc<S>,
    txn: &'a S::RoTxn<'a>,
}

impl<'a, I, S> Iterator for ShortestPathIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|item| {
            let start_node = item?;
            let (from_id, to_id) = match (self.path_type, start_node) {
                (PathType::From(from_id), TraversalVal::Node(to_node)) => (from_id, &to_node.id),
                (PathType::To(to_id), TraversalVal::Node(from_node)) => (&from_node.id, to_id),
                _ => return Err(GraphError::WrongTraversalValue),
            };

            self.storage
                .shortest_path(
                    self.txn,
                    self.edge_label.unwrap_or(""),
                    from_id,
                    to_id,
                )
                .map(TraversalVal::Path)
        })
    }
}

pub trait ShortestPathAdapter<'a, S: Storage + ?Sized, I: Iterator<Item = Result<TraversalVal, GraphError>>>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn shortest_path(
        self,
        edge_label: Option<&'a str>,
        from: Option<&'a u128>,
        to: Option<&'a u128>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        I: 'a;
}

impl<'a, I, S> ShortestPathAdapter<'a, S, I> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a,
    S: Storage + ?Sized,
{
    #[inline]
    fn shortest_path(
        self,
        edge_label: Option<&'a str>,
        from: Option<&'a u128>,
        to: Option<&'a u128>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>
    where
        I: 'a,
    {
        RoTraversalIterator {
            inner: ShortestPathIterator {
                iter: self.inner,
                path_type: match (from, to) {
                    (Some(from), None) => PathType::From(from),
                    (None, Some(to)) => PathType::To(to),
                    _ => panic!("Invalid shortest path: must provide either from or to"),
                },
                edge_label,
                storage: self.storage.clone(),
                txn: self.txn,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
