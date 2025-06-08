use super::tr_val::TraversalVal;
use crate::helix_engine::{
    graph_core::traversal_iter::{RoTraversalIterator, RwTraversalIterator},
    types::GraphError,
};
use crate::helix_storage::Storage;
use std::sync::Arc;

pub struct G {}

impl G {
    /// Starts a new empty traversal
    ///
    /// # Arguments
    ///
    /// * `storage` - An owned Arc of the storage for the traversal
    /// * `txn` - A reference to the transaction for the traversal
    ///
    /// # Example
    ///
    /// ```rust
    /// let storage = Arc::new(HelixGraphStorage::new());
    /// let txn = storage.graph_env.read_txn().unwrap();
    /// let traversal = G::new(storage, &txn);
    /// ```
    #[inline]
    pub fn new<'a, S: Storage + ?Sized>(
        storage: Arc<S>,
        txn: &'a S::RoTxn<'a>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        RoTraversalIterator {
            inner: std::iter::once(Ok(TraversalVal::Empty)),
            storage,
            txn,
        }
    }

    /// Starts a new traversal from a vector of traversal values
    ///
    /// # Arguments
    ///
    /// * `storage` - An owned Arc of the storage for the traversal
    /// * `txn` - A reference to the transaction for the traversal
    /// * `items` - A vector of traversal values to start the traversal from
    ///
    /// # Example
    ///
    /// ```rust
    /// let storage = Arc::new(HelixGraphStorage::new());
    /// let txn = storage.graph_env.read_txn().unwrap();
    /// let traversal = G::new_from(storage, &txn, vec![TraversalVal::Node(Node { id: 1, label: "Person".to_string(), properties: None })]);
    /// ```
    pub fn new_from<'a, S: Storage + ?Sized>(
        storage: Arc<S>,
        txn: &'a S::RoTxn<'a>,
        items: Vec<TraversalVal>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>, S> {
        RoTraversalIterator {
            inner: items.into_iter().map(|val| Ok(val)),
            storage,
            txn,
        }
    }

    /// Starts a new mutable traversal
    ///
    /// # Arguments
    ///
    /// * `storage` - An owned Arc of the storage for the traversal
    /// * `txn` - A reference to the transaction for the traversal
    /// * `items` - A vector of traversal values to start the traversal from
    ///
    /// # Example
    ///
    /// ```rust
    /// let storage = Arc::new(HelixGraphStorage::new());
    /// let txn = storage.graph_env.write_txn().unwrap();
    /// let traversal = G::new_mut(storage, &mut txn);
    /// ```
    pub fn new_mut<'scope, 'env, S: Storage + ?Sized>(
        storage: Arc<S>,
        txn: &'scope mut S::RwTxn<'env>,
    ) -> RwTraversalIterator<
        'scope,
        'env,
        impl Iterator<Item = Result<TraversalVal, GraphError>>,
        S,
    > {
        RwTraversalIterator {
            inner: std::iter::once(Ok(TraversalVal::Empty)),
            storage,
            txn,
        }
    }

    /// Starts a new mutable traversal from a vector of traversal values
    ///
    /// # Arguments
    ///
    /// * `storage` - An owned Arc of the storage for the traversal
    /// * `txn` - A reference to the transaction for the traversal
    /// * `items` - A vector of traversal values to start the traversal from        
    ///
    /// # Example
    ///
    /// ```rust
    /// let storage = Arc::new(HelixGraphStorage::new());
    /// let txn = storage.graph_env.write_txn().unwrap();
    /// let traversal = G::new_mut_from(storage, &mut txn, vec![TraversalVal::Node(Node { id: 1, label: "Person".to_string(), properties: None })]);
    /// ```
    pub fn new_mut_from<'scope, 'env, S: Storage + ?Sized>(
        storage: Arc<S>,
        txn: &'scope mut S::RwTxn<'env>,
        vals: Vec<TraversalVal>,
    ) -> RwTraversalIterator<
        'scope,
        'env,
        impl Iterator<Item = Result<TraversalVal, GraphError>>,
        S,
    > {
        RwTraversalIterator {
            inner: vals.into_iter().map(|val| Ok(val)),
            storage,
            txn,
        }
    }
}
