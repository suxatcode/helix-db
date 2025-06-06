use crate::helix_engine::graph_core::ops::in_::in_::InNodesIterator;
use crate::helix_engine::graph_core::ops::in_::in_e::InEdgesIterator;
use crate::helix_engine::graph_core::ops::out::out::OutNodesIterator;
use crate::helix_engine::graph_core::ops::out::out_e::OutEdgesIterator;
use crate::helix_engine::graph_core::ops::source::add_e::EdgeType;
use crate::helix_engine::graph_core::ops::source::n_from_type::NFromType;
use crate::helix_engine::graph_core::ops::tr_val::{Traversable, TraversalVal};
use crate::helix_engine::storage_core::storage_core::HelixGraphStorage;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::mcp::mcp::{MCPConnection, McpBackend};
use crate::helix_gateway::router::router::HandlerInput;
use crate::protocol::label_hash::hash_label;
use crate::protocol::response::Response;
use get_routes::local_handler;
use heed3::RoTxn;
use serde::{Deserialize, Deserializer};
use std::sync::Arc;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "tool_name", content = "args")]
pub enum ToolArgs {
    OutStep {
        edge_label: String,
        edge_type: EdgeType,
    },
    OutEStep {
        edge_label: String,
    },
    InStep {
        edge_label: String,
        edge_type: EdgeType,
    },
    InEStep {
        edge_label: String,
    },
    NFromType {
        node_type: String,
    },
}

pub(crate) trait ToolCalls<'a> {
    fn call(
        &'a self,
        txn: &'a RoTxn,
        connection_id: &'a MCPConnection,
        args: ToolArgs,
    ) -> Result<Vec<TraversalVal>, GraphError>;
}

impl<'a> ToolCalls<'a> for McpBackend {
    fn call(
        &'a self,
        txn: &'a RoTxn,
        connection: &'a MCPConnection,
        args: ToolArgs,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let result = match args {
            ToolArgs::OutStep {
                edge_label,
                edge_type,
            } => self.out_step(connection, &edge_label, &edge_type, txn),
            ToolArgs::OutEStep { edge_label } => self.out_e_step(connection, &edge_label, txn),
            ToolArgs::InStep {
                edge_label,
                edge_type,
            } => self.in_step(connection, &edge_label, &edge_type, txn),
            ToolArgs::InEStep { edge_label } => self.in_e_step(connection, &edge_label, txn),
            ToolArgs::NFromType { node_type } => self.n_from_type(&node_type, txn),
            _ => return Err(GraphError::New(format!("Tool {:?} not found", args))),
        }?;

        Ok(result)
    }
}

trait McpTools<'a> {
    fn out_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn out_e_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn in_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn in_e_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;

    fn n_from_type(
        &'a self,
        node_type: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError>;
}

impl<'a> McpTools<'a> for McpBackend {
    fn out_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key(&item.id(), &edge_label_hash);
                match db
                    .out_edges_db
                    .lazily_decode_data()
                    .get_duplicates(&txn, &prefix)
                {
                    Ok(Some(iter)) => Some(OutNodesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        edge_type,
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        match edge_type {
            EdgeType::Node => {}
            EdgeType::Vec => {}
        }

        let result = iter.take(100).collect();
        println!("result: {:?}", result);
        result
    }

    fn out_e_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key(&item.id(), &edge_label_hash);
                match db
                    .out_edges_db
                    .lazily_decode_data()
                    .get_duplicates(&txn, &prefix)
                {
                    Ok(Some(iter)) => Some(OutEdgesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        let result = iter.take(100).collect();
        println!("result: {:?}", result);
        result
    }

    fn in_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::in_edge_key(&item.id(), &edge_label_hash);
                match db
                    .in_edges_db
                    .lazily_decode_data()
                    .get_duplicates(&txn, &prefix)
                {
                    Ok(Some(iter)) => Some(InNodesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        edge_type,
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        match edge_type {
            EdgeType::Node => {}
            EdgeType::Vec => {}
        }

        let result = iter.take(100).collect();
        println!("result: {:?}", result);
        result
    }

    fn in_e_step(
        &'a self,
        connection: &'a MCPConnection,
        edge_label: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = connection
            .iter
            .clone()
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::in_edge_key(&item.id(), &edge_label_hash);
                match db
                    .in_edges_db
                    .lazily_decode_data()
                    .get_duplicates(&txn, &prefix)
                {
                    Ok(Some(iter)) => Some(InEdgesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        let result = iter.take(100).collect();
        println!("result: {:?}", result);
        result
    }

    fn n_from_type(
        &'a self,
        node_type: &'a str,
        txn: &'a RoTxn,
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let db = Arc::clone(&self.db);

        let iter = NFromType {
            iter: db.nodes_db.lazily_decode_data().iter(txn).unwrap(),
            label: node_type,
        };

        let result = iter.take(100).collect::<Result<Vec<_>, _>>();
        println!("result: {:?}", result);
        result
    }
}
