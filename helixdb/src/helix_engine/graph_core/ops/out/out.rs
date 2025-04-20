use std::sync::Arc;

use heed3::{types::Bytes, RoTxn, RwTxn};

use crate::helix_engine::{
    graph_core::traversal_iter::RoTraversalIterator,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};

use super::super::tr_val::{Traversable, TraversalVal};

pub struct OutNodesIterator<'a, T> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
    edge_label: &'a str,
}

// implementing iterator for OutIterator
impl<'a> Iterator for OutNodesIterator<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, value))) = self.iter.next() {
            let edge_id = std::str::from_utf8(value.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, edge_id) {
                if let Ok(node) = self.storage.get_node(self.txn, &edge.to_node) {
                    return Some(Ok(TraversalVal::Node(node)));
                }
            }
        }
        None
    }
}
impl<'a> Iterator for OutNodesIterator<'a, RwTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, value))) = self.iter.next() {
            let edge_id = std::str::from_utf8(value.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, edge_id) {
                if let Ok(node) = self.storage.get_node(self.txn, &edge.to_node) {
                    return Some(Ok(TraversalVal::Node(node)));
                }
            }
        }
        None
    }
}

pub struct OutNodes<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>, F, T>
where
    F: FnMut(Result<TraversalVal, GraphError>) -> OutNodesIterator<'a, T>,
    OutNodesIterator<'a, T>: std::iter::Iterator,
    T: 'a,
{
    iter: std::iter::Flatten<std::iter::Map<I, F>>,
}

impl<'a, I, F> Iterator for OutNodes<'a, I, F, RoTxn<'a>>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(Result<TraversalVal, GraphError>) -> OutNodesIterator<'a, RoTxn<'a>>,
{
    type Item = Result<TraversalVal, GraphError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, I, F> Iterator for OutNodes<'a, I, F, RwTxn<'a>>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(Result<TraversalVal, GraphError>) -> OutNodesIterator<'a, RwTxn<'a>>,
{
    type Item = Result<TraversalVal, GraphError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait OutAdapter<'a, T>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn out(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
    // where
    //     OutNodesIterator<'a, T>: std::iter::Iterator,
    //     T: 'a;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> OutAdapter<'a, RoTxn<'a>>
    for RoTraversalIterator<'a, I>
{
    fn out(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        {
            // iterate through the iterator and create a new iterator on the out edges
            let db = Arc::clone(&self.storage);
            let storage = Arc::clone(&self.storage);
            let txn = self.txn;
            let iter = self
                .inner
                .map(move |item| {
                    let prefix =
                        HelixGraphStorage::out_edge_key(item.unwrap().id(), edge_label, "");
                    let iter = db
                        .out_edges_db
                        .lazily_decode_data()
                        .prefix_iter(txn, &prefix)
                        .unwrap();

                    OutNodesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        txn,
                        edge_label,
                    }
                })
                .flatten();

            RoTraversalIterator {
                inner: OutNodes { iter },
                storage,
                txn,
            }
        }
    }
}

// impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> OutAdapter<'a, RwTxn<'a>>
//     for RwTraversalIterator<'a, I>
// {
//     fn out(
//         self,
//         edge_label: &'a str,
//     ) -> OutNodes<
//         'a,
//         Self,
//         impl FnMut(Result<TraversalVal, GraphError>) -> OutNodesIterator<'a, RwTxn<'a>>,
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

//                     OutNodesIterator {
//                         iter,
//                         storage: Arc::clone(&storage),
//                         txn,
//                         edge_label,
//                     }
//                 })
//                 .flatten();
//             OutNodes { iter }
//         }
//     }
// }
