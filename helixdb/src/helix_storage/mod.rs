pub mod lmdb_storage;

use crate::helix_engine::types::GraphError;
use crate::protocol::items::{Edge, Node};
use crate::protocol::value::Value;
use serde::Serialize;

pub trait DbRoTxn<'a>: Sized {}

pub trait DbRwTxn<'a>: DbRoTxn<'a> {
    fn commit(self) -> Result<(), GraphError>;
}

pub trait Storage: Send + Sync + 'static {
    type RoTxn<'a>: DbRoTxn<'a> + From<&'a mut Self::RwTxn<'a>>;
    type RwTxn<'a>: DbRwTxn<'a>;

    fn ro_txn(&self) -> Result<Self::RoTxn<'_>, GraphError>;
    fn rw_txn(&self) -> Result<Self::RwTxn<'_>, GraphError>;

    fn create_secondary_index(&mut self, name: &str) -> Result<(), GraphError>;
    fn drop_secondary_index(&mut self, name: &str) -> Result<(), GraphError>;

    fn check_exists<'a>(&self, txn: &Self::RoTxn<'a>, id: &u128) -> Result<bool, GraphError>;
    fn get_node<'a>(&self, txn: &Self::RoTxn<'a>, id: &u128) -> Result<Node, GraphError>;
    fn get_edge<'a>(&self, txn: &Self::RoTxn<'a>, id: &u128) -> Result<Edge, GraphError>;
    fn drop_node<'a>(&self, txn: &mut Self::RwTxn<'a>, id: &u128) -> Result<(), GraphError>;
    fn drop_edge<'a>(&self, txn: &mut Self::RwTxn<'a>, edge_id: &u128) -> Result<(), GraphError>;
    fn update_node<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        id: &u128,
        properties: &Value,
    ) -> Result<Node, GraphError>;
    fn update_edge<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        id: &u128,
        properties: &Value,
    ) -> Result<Edge, GraphError>;

    fn get_out_nodes<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        from_id: &u128,
    ) -> Result<Vec<Node>, GraphError>;

    fn get_in_nodes<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        to_id: &u128,
    ) -> Result<Vec<Node>, GraphError>;

    fn get_out_edges<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        from_id: &u128,
    ) -> Result<Vec<Edge>, GraphError>;

    fn get_in_edges<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        to_id: &u128,
    ) -> Result<Vec<Edge>, GraphError>;

    fn shortest_path<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        from_id: &u128,
        to_id: &u128,
    ) -> Result<(Vec<Node>, Vec<Edge>), GraphError>;

    fn shortest_mutual_path<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
        edge_label: &str,
        from_id: &u128,
        to_id: &u128,
    ) -> Result<(Vec<Node>, Vec<Edge>), GraphError>;

    fn add_edge<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        label: &str,
        properties: Option<Vec<(String, Value)>>,
        from_node: u128,
        to_node: u128,
    ) -> Result<Edge, GraphError>;

    fn node_from_index<'a, K>(
        &self,
        txn: &Self::RoTxn<'a>,
        index: &str,
        key: K,
    ) -> Result<Option<Node>, GraphError>
    where
        K: Into<Value> + Serialize;

    fn add_node<'a>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        label: &str,
        properties: Option<Vec<(String, Value)>>,
        secondary_indices: Option<&[&str]>,
    ) -> Result<Node, GraphError>;

    fn get_all_nodes<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<Node, GraphError>> + 'a>, GraphError>;

    fn get_all_edges<'a>(
        &self,
        txn: &Self::RoTxn<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<Edge, GraphError>> + 'a>, GraphError>;

    fn index_node<'a, K>(
        &self,
        txn: &mut Self::RwTxn<'a>,
        index: &str,
        key: K,
        node: &Node,
    ) -> Result<(), GraphError>
    where
        K: Into<Value> + Serialize;
} 