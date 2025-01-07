use bindings::js_binding::HelixJS;
use chrono::Utc;
use helix_engine::{
    graph_core::{
        graph_core::HelixGraphEngine,
        traversal::TraversalBuilder,
        traversal_steps::{TraversalMethods, TraversalSteps},
    },
    props,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use helix_gateway::{
    router::router::{HandlerFn, HandlerInput, HandlerSubmission, HelixRouter, RouterError},
    GatewayOpts, HelixGateway,
};
use inventory;
use protocol::{request::Request, response::Response};
use rand::Rng;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};

use helixc::parser::helix_parser::{Expression, GraphStep, HelixParser, Source, StartNode, Step};

pub mod bindings;

uniffi::include_scaffolding!("helix");

pub struct HelixEmbedded {
    graph: Arc<HelixGraphEngine>,
}

#[derive(Debug)]
pub enum QueryInput {
    StringValue { value: String },
    IntegerValue { value: i32 },
    FloatValue { value: f64 },
    BooleanValue { value: bool },
}


impl HelixEmbedded {
    pub fn new(user: String) -> Result<Self, GraphError> {
        let home_dir = dirs::home_dir().ok_or(GraphError::New(
            "Unable to determine home directory".to_string(),
        ))?;
        let path = format!("{}/.helix/graph_data/{}", home_dir.display(), user);
        println!("Path: {:?}", path);
        let graph = Arc::new(HelixGraphEngine::new(path.as_str()).unwrap());
        Ok(Self { graph })
    }

    pub fn query(&self, query: String, params: Vec<QueryInput>) -> Result<String, HelixLiteError> {
        let ast = HelixParser::parse_source(query.as_str()).unwrap();
        let mut return_vals: HashMap<String, String> = HashMap::new();
        let mut vars: HashMap<String, QueryInput> = HashMap::new();

        for query in ast.queries {
            for stmt in query.statements {
                match stmt.value {
                    Expression::Traversal(tr) | Expression::Exists(tr) => {
                        // build traversal based on steps with traversal builder
                        // initialise from start node
                        // step through all steps and execute.
                        let (start_types, start_ids) = match tr.start {
                            StartNode::Vertex { types, ids } | StartNode::Edge { types, ids } => {
                                let types = match types {
                                    Some(types) => types,
                                    None => vec![],
                                };
                                let ids = match ids {
                                    Some(ids) => ids,
                                    None => vec![],
                                };
                                (types, ids)
                            }
                            _ => unreachable!(),
                        };
                        let start_nodes = start_ids
                            .iter()
                            .map(|id| self.graph.storage.get_node(id).map_err(GraphError::from))
                            .collect::<Result<Vec<_>, GraphError>>()?;
                        let mut tr_builder =
                            TraversalBuilder::new(&self.graph.storage, start_nodes);

                        for step in tr.steps {
                            match step {
                                Step::Vertex(graph_step) => match graph_step {
                                    GraphStep::Out(labels) => match labels {
                                        Some(l) => {
                                            if l.len() > 1 {
                                                return Err(HelixLiteError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
                                            }
                                            if let Some(label) = l.first() {
                                                tr_builder.out(label);
                                            }
                                        }
                                        None => {
                                            tr_builder.out("");
                                        }
                                    },
                                    GraphStep::In(labels) => match labels {
                                        Some(l) => {
                                            if l.len() > 1 {
                                                return Err(HelixLiteError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
                                            }
                                            if let Some(label) = l.first() {
                                                tr_builder.in_(label);
                                            }
                                        }
                                        None => {
                                            tr_builder.in_("");
                                        }
                                    },
                                    GraphStep::OutE(labels) => match labels {
                                        Some(l) => {
                                            if l.len() > 1 {
                                                return Err(HelixLiteError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
                                            }
                                            if let Some(label) = l.first() {
                                                tr_builder.out_e(label);
                                            }
                                        }
                                        None => {
                                            tr_builder.out_e("");
                                        }
                                    },
                                    GraphStep::InE(labels) => match labels {
                                        Some(l) => {
                                            if l.len() > 1 {
                                                return Err(HelixLiteError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
                                            }
                                            if let Some(label) = l.first() {
                                                tr_builder.in_e(label);
                                            }
                                        }
                                        None => {
                                            tr_builder.in_e("");
                                        }
                                    },
                                    GraphStep::Both(labels) => match labels {
                                        Some(l) => {
                                            if l.len() > 1 {
                                                return Err(HelixLiteError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
                                            }
                                            if let Some(label) = l.first() {
                                                tr_builder.both(label);
                                            }
                                        }
                                        None => {
                                            tr_builder.both("");
                                        }
                                    },
                                    GraphStep::BothE(labels) => match labels {
                                        Some(l) => {
                                            if l.len() > 1 {
                                                return Err(HelixLiteError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
                                            }
                                            if let Some(label) = l.first() {
                                                tr_builder.both_e(label);
                                            }
                                        }
                                        None => {
                                            tr_builder.both_e("");
                                        }
                                    },
                                    _ => unreachable!(),
                                },
                                Step::Edge(graph_step) => match graph_step {
                                    GraphStep::OutV => {
                                        tr_builder.out_v();
                                    }
                                    GraphStep::InV => {
                                        tr_builder.in_v();
                                    }
                                    GraphStep::BothV => {
                                        tr_builder.both_v();
                                    }
                                    _ => unreachable!(),
                                },
                                Step::Count => {
                                    tr_builder.count();
                                }
                                // Step::Props(property_names) => {
                                //     tr_builder.get_properties(&property_names);
                                // },
                                // Step::Where(expression) => {
                                //     tr_builder.filter(|val| {
                                //         // Need to implement evaluation of expression against TraversalValue
                                //         evaluate_expression(expression, val)
                                //     });
                                // },
                                _ => unreachable!(),
                            }
                        }

                        return_vals
                            .insert(stmt.variable, self.graph.result_to_json_string(&tr_builder))
                            .unwrap();
                    }
                    _ => {
                        // insert variable to hashmap
                        let var = match stmt.value {
                            Expression::StringLiteral(value) => QueryInput::StringValue { value },
                            Expression::NumberLiteral(value) => QueryInput::IntegerValue { value },
                            Expression::BooleanLiteral(value) => QueryInput::BooleanValue { value },
                            _ => unreachable!(),
                        };
                        vars.insert(stmt.variable, var);
                    }
                }
            }
        }

        let json_string = serde_json::to_string_pretty(&return_vals).unwrap();
        Ok(json_string)
    }
}

#[derive(Debug)]
pub enum HelixLiteError {
    Default(String),
}

impl std::fmt::Display for HelixLiteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HelixLiteError::Default(msg) => write!(f, "Graph error: {}", msg),
        }
    }
}

impl From<RouterError> for HelixLiteError {
    fn from(error: RouterError) -> Self {
        HelixLiteError::Default(error.to_string())
    }
}

impl From<GraphError> for HelixLiteError {
    fn from(error: GraphError) -> Self {
        HelixLiteError::Default(error.to_string())
    }
}

impl From<&'static str> for HelixLiteError {
    fn from(error: &'static str) -> Self {
        HelixLiteError::Default(error.to_string())
    }
}
