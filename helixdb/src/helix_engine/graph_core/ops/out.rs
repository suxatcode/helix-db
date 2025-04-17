use std::sync::Arc;

use heed3::{types::Bytes, RoTxn};

use crate::{
    decode_str,
    helix_engine::storage_core::{
        storage_core::HelixGraphStorage, storage_methods::StorageMethods,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

use super::tr_val::{Traversable, TraversalVal};

pub struct OutNodes<'a> {
    iter: heed3::RoPrefix<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    edge_label: &'a str,
}

// implementing iterator for OutIterator
impl<'a> Iterator for OutNodes<'a> {
    type Item = TraversalVal;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, value))) = self.iter.next() {
            let edge_id = std::str::from_utf8(value.decode().unwrap()).unwrap();
            if let Ok(edge) = self.storage.get_edge(self.txn, edge_id) {
                if self.edge_label.is_empty() || edge.label == self.edge_label {
                    if let Ok(node) = self.storage.get_node(self.txn, &edge.to_node) {
                        return Some(TraversalVal::Node(node));
                    }
                }
            }
        }
        None
    }
}

pub struct Out<'a, I: Iterator<Item = TraversalVal>, F>
where
    F: FnMut(TraversalVal) -> OutNodes<'a>,
{
    iter: std::iter::Flatten<std::iter::Map<I, F>>,
}

impl<'a, I, F> Iterator for Out<'a, I, F>
where
    I: Iterator<Item = TraversalVal>,
    F: FnMut(TraversalVal) -> OutNodes<'a>,
{
    type Item = TraversalVal;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait OutAdapter: Iterator {
    fn out<'a>(
        self,
        db: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
        edge_label: &'a str,
    ) -> Out<'a, Self, impl FnMut(TraversalVal) -> OutNodes<'a>>
    where
        Self: Sized + Iterator<Item = TraversalVal> + 'a,
        Self::Item: Send,
    {
        // iterate through the iterator and create a new iterator on the out edges

        let db = Arc::clone(&db);
        let iter = self
            .map(move |item| out_nodes(item, db.clone(), txn, edge_label))
            .flatten();
        // println!("{:?}",
        //     iter.clone()
        // );
        Out { iter }
    }
}

pub fn out_nodes<'a>(
    item: TraversalVal,
    db: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    edge_label: &'a str,
) -> OutNodes<'a> {
    let prefix = [b"o:", item.id().as_bytes(), b":", edge_label.as_bytes()].concat();
    let iter = db
        .out_edges_db
        .lazily_decode_data()
        .prefix_iter(txn, &prefix)
        .unwrap();

    OutNodes {
        iter,
        storage: db,
        txn,
        edge_label,
    }
}

impl<T: ?Sized> OutAdapter for T where T: Iterator {}

// pub struct OutEdges<'a, I> {
//     iter: I,
//     storage: Arc<HelixGraphStorage>,
//     txn: RoTxn<'a>,
//     edge_label: String,
// }

// // implementing iterator for OutIterator
// impl<'a, I> Iterator for OutEdges<'a, I>
// where
//     I: Iterator<Item = TraversalVal>,
// {
//     type Item = Vec<TraversalVal>;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.iter.next().map(|item| {
//             // running storage function to get out nodes
//             match item {
//                 TraversalVal::Node(node) => {
//                     let nodes = self
//                         .storage
//                         .get_out_edges(&self.txn, &node.id, &self.edge_label);
//                     assert!(nodes.is_ok()); // ASSERTS IN PROD LETS GO
//                     nodes.unwrap_or(vec![TraversalVal::Empty])
//                 }
//                 _ => vec![TraversalVal::Empty],
//             }
//         })
//     }
// }

// pub trait OutEdgesAdapter: Iterator {
//     fn out_edges(
//         self,
//         db: Arc<HelixGraphStorage>,
//         txn: RoTxn<'_>,
//         edge_label: String,
//     ) -> OutEdges<Self>
//     where
//         Self: Sized + Iterator,
//         Self::Item: Send,
//     {
//         OutEdges {
//             iter: self,
//             storage: db,
//             txn,
//             edge_label,
//         }
//     }
// }

// impl<T: ?Sized> OutEdgesAdapter for T where T: Iterator {}
