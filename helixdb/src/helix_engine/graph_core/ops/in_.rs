// use std::sync::Arc;

// use heed3::RoTxn;

// use crate::{
//     helix_engine::storage_core::{
//         storage_core::HelixGraphStorage, storage_methods::StorageMethods,
//     },
//     protocol::{
//         filterable::{Filterable, FilterableType},
//         items::{Edge, Node},
//     },
// };

// use super::tr_val::TraversalVal;

// pub struct In<'a, I> {
//     iter: I,
//     storage: Arc<HelixGraphStorage>,
//     txn: RoTxn<'a>,
//     edge_label: String,
// }

// // implementing iterator for OutIterator
// impl<'a, I> Iterator for In<'a, I>
// where
// I: Iterator<Item = TraversalVal>,
// {
//     type Item = Vec<TraversalVal>;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.iter.next().map(|item| {
//             // running storage function to get out nodes
//             match item {
//                 TraversalVal::Node(node) => {
//                     let nodes = self
//                         .storage
//                         .get_in_nodes(&self.txn, &node.id, &self.edge_label);
//                     assert!(nodes.is_ok()); // ASSERTS IN PROD LETS GO
//                     nodes.unwrap_or(vec![TraversalVal::Empty])
//                 }
//                 _ => vec![TraversalVal::Empty],
//             }
//         })
//     }
// }

// pub trait InAdapter: Iterator {
//     fn in_(
//         self,
//         db: Arc<HelixGraphStorage>,
//         txn: RoTxn<'_>,
//         edge_label: String,
//     ) -> In<Self>
//     where
//         Self: Sized + Iterator,
//         Self::Item: Send,
//     {
//         In {
//             iter: self,
//             storage: db,
//             txn,
//             edge_label,
//         }
//     }
// }

// impl<T: ?Sized> InAdapter for T where T: Iterator {}



// pub struct InEdges<'a, I> {
//     iter: I,
//     storage: Arc<HelixGraphStorage>,
//     txn: RoTxn<'a>,
//     edge_label: String,
// }

// // implementing iterator for InIterator
// impl<'a, I> Iterator for InEdges<'a, I>
// where
//     I: Iterator<Item = TraversalVal>,
// {
//     type Item = Vec<TraversalVal>;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.iter.next().map(|item| {
//             // running storage function to get out nodes
//             match item {
//                 TraversalVal::Node(edge) => {
//                     let edges = self
//                         .storage
//                         .get_in_edges(&self.txn, &edge.id, &self.edge_label);
//                     assert!(edges.is_ok()); // ASSERTS IN PROD LETS GO
//                     edges.unwrap_or(vec![TraversalVal::Empty])
//                 }
//                 _ => vec![TraversalVal::Empty],
//             }
//         })
//     }
// }

// pub trait InEdgesAdapter: Iterator {
//     fn in_edges(
//         self,
//         db: Arc<HelixGraphStorage>,
//         txn: RoTxn<'_>,
//         edge_label: String,
//     ) -> InEdges<Self>
//     where
//         Self: Sized + Iterator,
//         Self::Item: Send,
//     {
//         InEdges {
//             iter: self,
//             storage: db,
//             txn,
//             edge_label,
//         }
//     }
// }

// impl<T: ?Sized> InEdgesAdapter for T where T: Iterator {}
