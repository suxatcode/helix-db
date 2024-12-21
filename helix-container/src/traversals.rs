use helix_engine::graph_core::traversal::TraversalBuilder;
use helix_engine::graph_core::traversal_steps::{SourceTraversalSteps, TraversalSteps};
use get_routes::handler;
use helix_engine::props;
use helix_engine::storage_core::storage_core::HelixGraphStorage;
use helix_engine::storage_core::storage_methods::StorageMethods;
use helix_gateway::router::router::{HandlerInput, RouterError};
use inventory;
use protocol::response::Response;
use rand::Rng;

#[handler]
pub fn test_function2(input: &HandlerInput, response: &mut Response) -> Result<(), RouterError> {
    let graph = &input.graph.lock().unwrap();

    let mut traversal = TraversalBuilder::new(vec![]);
    traversal.v(&graph.storage);
    traversal.out(&graph.storage, "knows");
    response.body = graph.result_to_utf8(&traversal);
    Ok(())
}

