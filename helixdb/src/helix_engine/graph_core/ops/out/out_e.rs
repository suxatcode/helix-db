use std::sync::Arc;

use heed3::{types::Bytes, RoTxn, RwTxn};

use crate::helix_engine::{
    graph_core::traversal_iter::RoTraversalIterator,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};

use super::super::tr_val::{Traversable, TraversalVal};

pub struct OutEdgesIterator<'a, T> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
}

// implementing iterator for OutIterator
impl<'a> Iterator for OutEdgesIterator<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, value))) = self.iter.next() {
            let edge_id = HelixGraphStorage::get_u128_from_bytes(value.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, &edge_id) {
                return Some(Ok(TraversalVal::Edge(edge)));
            }
        }
        None
    }
}
impl<'a> Iterator for OutEdgesIterator<'a, RwTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, value))) = self.iter.next() {
            let edge_id = HelixGraphStorage::get_u128_from_bytes(value.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, &edge_id) {
                return Some(Ok(TraversalVal::Edge(edge)));
            }
        }
        None
    }
}

pub struct OutEdges<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>, F, T>
where
    F: FnMut(Result<TraversalVal, GraphError>) -> OutEdgesIterator<'a, T>,
    OutEdgesIterator<'a, T>: std::iter::Iterator,
    T: 'a,
{
    iter: std::iter::Flatten<std::iter::Map<I, F>>,
}

impl<'a, I, F> Iterator for OutEdges<'a, I, F, RoTxn<'a>>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(Result<TraversalVal, GraphError>) -> OutEdgesIterator<'a, RoTxn<'a>>,
{
    type Item = Result<TraversalVal, GraphError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, I, F> Iterator for OutEdges<'a, I, F, RwTxn<'a>>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(Result<TraversalVal, GraphError>) -> OutEdgesIterator<'a, RwTxn<'a>>,
{
    type Item = Result<TraversalVal, GraphError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait OutEdgesAdapter<'a, T>:
    Iterator<Item = Result<TraversalVal, GraphError>> + Sized
{
    fn out_e(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> OutEdgesAdapter<'a, RoTxn<'a>>
    for RoTraversalIterator<'a, I>
{
    fn out_e(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        // iterate through the iterator and create a new iterator on the out edges
        let db = Arc::clone(&self.storage);
        let storage = Arc::clone(&self.storage);
        let txn = self.txn;
        let iter = self
            .inner
            .map(move |item| {
                let item = item.unwrap();
                let prefix = HelixGraphStorage::out_edge_key(&item.id(), edge_label, None);
                let iter = db
                    .out_edges_db
                    .lazily_decode_data()
                    .prefix_iter(txn, &prefix)
                    .unwrap();

                OutEdgesIterator {
                    iter,
                    storage: Arc::clone(&db),
                    txn,
                }
            })
            .flatten();
        RoTraversalIterator {
            inner: OutEdges { iter },
            storage,
            txn,
        }
    }
}

// impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> OutEdgesAdapter<'a, RwTxn<'a>>
//     for RwTraversalIterator<'a, I>
// {
//     fn out_edges(
//         self,
//         edge_label: &'a str,
//     ) -> OutEdges<
//         'a,
//         Self,
//         impl FnMut(Result<TraversalVal, GraphError>) -> OutEdgesIterator<'a, RwTxn<'a>>,
//         RwTxn<'a>,
//     > {
//         {
//             // iterate through the iterator and create a new iterator on the out edges
//             let db = Arc::clone(&self.storage);
//             let storage = Arc::clone(&self.storage);
//             let txn = self.txn;
//             let iter = self
//                 .map(move |item| {
//                     let prefix = HelixGraphStorage::out_edge_key(item.unwrap().id(), "");
//                     let iter = db
//                         .out_edges_db
//                         .lazily_decode_data()
//                         .prefix_iter(txn, &prefix)
//                         .unwrap();

//                     OutEdgesIterator {
//                         iter,
//                         storage: Arc::clone(&storage),
//                         txn,
//                         edge_label,
//                     }
//                 })
//                 .flatten();
//             OutEdges { iter }
//         }
//     }
// }
