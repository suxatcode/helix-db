// provides tool endpoints for mcp
// init endpoint to get a user id and establish a connection to helix server

// wraps iter in new tools

use std::{collections::HashMap, sync::Arc, vec::IntoIter};

use get_routes::local_handler;
use heed3::{AnyTls, RoTxn};
use serde::Deserialize;

use crate::{
    helix_engine::{
        graph_core::{
            graph_core::HelixGraphEngine,
            ops::{
                in_::{in_::InNodesIterator, in_e::InEdgesIterator},
                out::{out::OutNodesIterator, out_e::OutEdgesIterator},
                source::{add_e::EdgeType, n_from_type::NFromType},
                tr_val::{Traversable, TraversalVal},
            },
            traversal_iter::RoTraversalIterator,
        },
        storage_core::storage_core::HelixGraphStorage,
        types::GraphError,
    },
    helix_gateway::{mcp::{mcp::ToolCallRequest, tools::ToolArgs}, router::router::HandlerInput},
    protocol::{label_hash::hash_label, response::Response},
};


#[local_handler]
pub fn call_tool(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: ToolCallRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    Ok(())
}