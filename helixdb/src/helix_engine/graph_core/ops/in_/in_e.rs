use std::sync::Arc;

use heed3::{types::Bytes, RoTxn, RwTxn};

use crate::helix_engine::{
    graph_core::traversal_iter::RoTraversalIterator,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};

use super::super::tr_val::{Traversable, TraversalVal};

pub struct InEdgesIterator<'a, T> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
}

// implementing iterator for OutIterator
impl<'a> Iterator for InEdgesIterator<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, data))) = self.iter.next() {
            let (_, edge_id) =
                HelixGraphStorage::unpack_adj_edge_data(&data.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, &edge_id) {
                return Some(Ok(TraversalVal::Edge(edge)));
            }
        }
        None
    }
}
impl<'a> Iterator for InEdgesIterator<'a, RwTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, data))) = self.iter.next() {
            let (_, edge_id) =
                HelixGraphStorage::unpack_adj_edge_data(&data.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, &edge_id) {
                return Some(Ok(TraversalVal::Edge(edge)));
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

pub trait InEdgesAdapter<'a, T>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn in_e(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> InEdgesAdapter<'a, RoTxn<'a>>
    for RoTraversalIterator<'a, I>
{
    fn in_e(
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
                let label_hash = HelixGraphStorage::hash_label(edge_label);
                let prefix = HelixGraphStorage::in_edge_key(&item.unwrap().id(), &label_hash);
                let iter = db
                    .in_edges_db
                    .lazily_decode_data()
                    .prefix_iter(txn, &prefix)
                    .unwrap();

                InEdgesIterator {
                    iter,
                    storage: Arc::clone(&db),
                    txn,
                }
            })
            .flatten();

        RoTraversalIterator {
            inner: InEdges { iter },
            storage,
            txn,
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
