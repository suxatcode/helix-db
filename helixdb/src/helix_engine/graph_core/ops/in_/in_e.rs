use std::sync::Arc;

use heed3::{types::Bytes, RoTxn, RwTxn};

use crate::{
    decode_str,
    helix_engine::{
        graph_core::traversal_iter::{RoTraversalIterator},
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

use super::super::tr_val::{Traversable, TraversalVal};

pub struct InEdgesIterator<'a, T> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
    edge_label: &'a str,
}

// implementing iterator for OutIterator
impl<'a> Iterator for InEdgesIterator<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, value))) = self.iter.next() {
            let edge_id = std::str::from_utf8(value.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, edge_id) {
                if self.edge_label.is_empty() || edge.label == self.edge_label {
                    return Some(Ok(TraversalVal::Edge(edge)));
                }
            }
        }
        None
    }
}
impl<'a> Iterator for InEdgesIterator<'a, RwTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, value))) = self.iter.next() {
            let edge_id = std::str::from_utf8(value.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, edge_id) {
                if self.edge_label.is_empty() || edge.label == self.edge_label {
                    return Some(Ok(TraversalVal::Edge(edge)));
                }
            }
        }
        None
    }
}

pub struct InEdges<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>, F, T>
where
    F: FnMut(Result<TraversalVal, GraphError>) -> InEdgesIterator<'a, T>,
    InEdgesIterator<'a, T>: std::iter::Iterator,
    T: 'a,
{
    iter: std::iter::Flatten<std::iter::Map<I, F>>,
}

impl<'a, I, F> Iterator for InEdges<'a, I, F, RoTxn<'a>>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(Result<TraversalVal, GraphError>) -> InEdgesIterator<'a, RoTxn<'a>>,
{
    type Item = Result<TraversalVal, GraphError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, I, F> Iterator for InEdges<'a, I, F, RwTxn<'a>>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(Result<TraversalVal, GraphError>) -> InEdgesIterator<'a, RwTxn<'a>>,
{
    type Item = Result<TraversalVal, GraphError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait InEdgesAdapter<'a, T>:
    Iterator<Item = Result<TraversalVal, GraphError>> + Sized
{
    fn in_edges(
        self,
        edge_label: &'a str,
    ) -> InEdges<
        'a,
        Self,
        impl FnMut(Result<TraversalVal, GraphError>) -> InEdgesIterator<'a, T>,
        T,
    >
    where
        InEdgesIterator<'a, T>: std::iter::Iterator;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> InEdgesAdapter<'a, RoTxn<'a>>
    for RoTraversalIterator<'a, I>
{
    fn in_edges(
        self,
        edge_label: &'a str,
    ) -> InEdges<
        'a,
        Self,
        impl FnMut(Result<TraversalVal, GraphError>) -> InEdgesIterator<'a, RoTxn<'a>>,
        RoTxn<'a>,
    > {
        {
            // iterate through the iterator and create a new iterator on the out edges
            let db = Arc::clone(&self.storage);
            let storage = Arc::clone(&self.storage);
            let txn = self.txn;
            let iter = self
                .map(move |item| {
                    let prefix = HelixGraphStorage::out_edge_key(item.unwrap().id(), "");
                    let iter = db
                        .in_edges_db
                        .lazily_decode_data()
                        .prefix_iter(txn, &prefix)
                        .unwrap();

                    InEdgesIterator {
                        iter,
                        storage: Arc::clone(&storage),
                        txn,
                        edge_label,
                    }
                })
                .flatten();
            InEdges { iter }
        }
    }
}

// impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> InEdgesAdapter<'a, RwTxn<'a>>
//     for RwTraversalIterator<'a, I>
// {
//     fn in_edges(
//         self,
//         edge_label: &'a str,
//     ) -> InEdges<
//         'a,
//         Self,
//         impl FnMut(Result<TraversalVal, GraphError>) -> InEdgesIterator<'a, RwTxn<'a>>,
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
//                         .in_edges_db
//                         .lazily_decode_data()
//                         .prefix_iter(txn, &prefix)
//                         .unwrap();

//                     InEdgesIterator {
//                         iter,
//                         storage: Arc::clone(&storage),
//                         txn,
//                         edge_label,
//                     }
//                 })
//                 .flatten();
//             InEdges { iter }
//         }
//     }
// }
