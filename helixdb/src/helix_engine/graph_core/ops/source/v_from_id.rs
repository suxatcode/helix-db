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

pub struct VFromId<'a> {
    iter: std::iter::Empty<()>,
    storage: &'a Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    id: &'a str,
}
// implementing iterator for OutIterator
impl<'a> Iterator for VFromId<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let node: Node = match self.storage.get_node(self.txn, self.id) {
                Ok(node) => node,
                Err(e) => return Err(e),
            };
            Ok(TraversalVal::Node(node))
        })
    }
}

impl<'a> VFromId<'a> {
    pub fn new(storage: &'a Arc<HelixGraphStorage>, txn: &'a RoTxn, id: &'a str) -> Self {
        VFromId {
            iter: std::iter::empty(),
            storage,
            txn,
            id,
        }
    }
}
