use std::{iter::Once, sync::Arc};

use heed3::{
    types::{Bytes, Lazy, Unit},
    RoTxn,
};

use crate::{
    helix_engine::{
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

use super::super::tr_val::TraversalVal;

pub struct EFromId<'a> {
    iter: std::iter::Empty<()>,
    storage: &'a Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    id: &'a str,
}
// implementing iterator for OutIterator
impl<'a> Iterator for EFromId<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let edge: Edge = match self.storage.get_edge(self.txn, self.id) {
                Ok(edge) => edge,
                Err(e) => return Err(e),
            };
            Ok(TraversalVal::Edge(edge))
        })
    }
}

impl<'a> EFromId<'a> {
    pub fn new(storage: &'a Arc<HelixGraphStorage>, txn: &'a RoTxn, id: &'a str) -> Self {
        EFromId {
            iter: std::iter::empty(),
            storage,
            txn,
            id,
        }
    }
}
