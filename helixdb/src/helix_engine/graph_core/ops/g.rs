use std::sync::Arc;

use heed3::{types::Bytes, RoTxn, RwTxn};

use crate::{
    decode_str,
    helix_engine::{
        graph_core::traversal_iter::{RoTraversalIterator, RwTraversalIterator},
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

use super::tr_val::{Traversable, TraversalVal};

pub struct G {
    iter: std::iter::Once<Result<TraversalVal, GraphError>>,
}

// implementing iterator for OutIterator
impl Iterator for G {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl G {
    pub fn new<'a>(
        storage: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        Self: Sized,
    {
        let iter = std::iter::once(Ok(TraversalVal::Empty));
        RoTraversalIterator {
            inner: iter,
            storage,
            txn,
        }
    }

    pub fn new_mut<'a, 'b>(
        storage: Arc<HelixGraphStorage>,
        txn: &'b mut RwTxn<'a>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        Self: Sized,
    {
        let iter = std::iter::once(Ok(TraversalVal::Empty));
        RwTraversalIterator {
            inner: iter,
            storage,
            txn,
        }
    }

    pub fn new_from<'a>(
        storage: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
        vals: Vec<TraversalVal>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        RoTraversalIterator {
            inner: vals.into_iter().map(|val| Ok(val)),
            storage,
            txn,
        }
    }
}
