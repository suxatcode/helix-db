use std::sync::Arc;

use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
    RoTxn,
};

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RoTraversalIterator,
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::items::SerializedEdge,
};

use super::super::tr_val::TraversalVal;

pub struct EFromType<'a> {
    iter: heed3::RoIter<'a, U128<BE>, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    label: &'a str,
}
// implementing iterator for OutIterator
impl<'a> Iterator for EFromType<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(value) = self.iter.next() {
            let (key, value) = value.unwrap();
            match value.decode() {
                Ok(value) => match SerializedEdge::decode_edge(&value, key) {
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
pub trait EFromTypeAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn e_from_type(
        self,
        label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}
impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> EFromTypeAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn e_from_type(
        self,
        label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let db = self.storage.clone();
        let txn: &RoTxn<'_> = self.txn;
        let iter = db.edges_db.lazily_decode_data().iter(txn).unwrap();
        RoTraversalIterator {
            inner: EFromType {
                iter,
                storage: db.clone(),
                txn,
                label,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
