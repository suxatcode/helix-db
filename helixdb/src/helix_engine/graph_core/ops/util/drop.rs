use std::sync::Arc;

use heed3::RwTxn;

use crate::{
    helix_engine::storage_core::{
        storage_core::HelixGraphStorage, storage_methods::StorageMethods,
    },
    protocol::filterable::{Filterable, FilterableType},
};

pub struct Drop<'a, I> {
    iter: I,
    storage: Arc<HelixGraphStorage>,
    txn: RwTxn<'a>,
}

// implementing iterator for Drop
impl<'a, I> Iterator for Drop<'a, I>
where
    I: Iterator<Item: Filterable>,
{
    type Item = ();

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => {
                match item.type_name() {
                    FilterableType::Node => {
                        self.storage.drop_node(&mut self.txn, &item.id()).unwrap();
                    }
                    FilterableType::Edge => {
                        self.storage.drop_edge(&mut self.txn, &item.id()).unwrap();
                    }
                    // FilterableType::Vector => self.storage.drop_vector(&self.txn, &item.id());
                    _ => {
                        return None;
                    }
                }
                Some(())
            }
            None => None,
        }
    }
}

pub trait DropAdapter: Iterator {
    fn drop(self, db: Arc<HelixGraphStorage>, txn: RwTxn<'_>) -> Drop<Self>
    where
        Self: Sized + Iterator,
        Self::Item: Send,
    {
        Drop {
            iter: self,
            storage: db,
            txn,
        }
    }
}

impl<T: ?Sized> DropAdapter for T where T: Iterator {}
