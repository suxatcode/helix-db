use get_routes::handler;
use helix_engine::graph_core::traversal::TraversalBuilder;
use helix_engine::graph_core::traversal_steps::{
    SourceTraversalSteps, TraversalMethods,
};
use helix_engine::types::GraphError;
use helix_gateway::router::router::HandlerInput;
use protocol::response::Response;
use protocol::traversal_value::TraversalValue;
use protocol::{Filterable, Value};
use serde::Deserialize;

#[derive(Deserialize)]
struct Data {
    screen_name: String,
}

#[handler]
pub fn get_users(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let data: Data = serde_json::from_slice(&input.request.body).unwrap();

    let mut tr = TraversalBuilder::new(&input.graph.storage, TraversalValue::Empty);
    tr.v().filter_nodes(|node| {
        if let Some(val) = node.check_property("screen_name") {
            match val {
                Value::String(s) => Ok(*s == data.screen_name),
                _ => unreachable!(),
            }
        } else {
            Err(GraphError::from("Invalid node".to_string()))
        }
    });
    response.body = input.graph.result_to_json(&tr);
    Ok(())
}

#[handler]
pub fn get_all_users(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let mut tr = TraversalBuilder::new(&input.graph.storage, TraversalValue::Empty);
    tr.v();
    response.body = input.graph.result_to_json(&tr);
    Ok(())
}
