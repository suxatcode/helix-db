use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        types::GraphError,
    },
    helix_storage::Storage,
    protocol::items::Edge,
};
use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
};

pub struct EFromType<'a> {
    pub iter: heed3::RoIter<'a, U128<BE>, heed3::types::LazyDecode<Bytes>>,
    pub label: &'a str,
}

impl<'a> Iterator for EFromType<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(value) = self.iter.next() {
            let (key, value) = value.unwrap();
            match value.decode() {
                Ok(value) => match Edge::decode_edge(&value, key) {
                    Ok(edge) => match &edge.label {
                        label if label == self.label => return Some(Ok(TraversalVal::Edge(edge))),
                        _ => continue,
                    },
                    Err(e) => return Some(Err(GraphError::ConversionError(e.to_string()))),
                },
                Err(e) => return Some(Err(GraphError::ConversionError(e.to_string()))),
            }
        }
        None
    }
}
pub trait EFromTypeAdapter<'a, S: Storage + ?Sized>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    fn e_from_type(
        self,
        label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S>;
}
impl<'a, I, S> EFromTypeAdapter<'a, S> for RoTraversalIterator<'a, I, S>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    S: Storage + ?Sized,
{
    #[inline]
    fn e_from_type(
        self,
        label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        let iter = self.storage.get_all_edges(self.txn).unwrap();
        RoTraversalIterator {
            inner: EFromType { iter, label },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
