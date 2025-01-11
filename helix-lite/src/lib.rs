use bindings::js_binding::HelixJS;
use chrono::Utc;
use helix_engine::{
    graph_core::{
        graph_core::HelixGraphEngine,
        traversal::TraversalBuilder,
        traversal_steps::{SourceTraversalSteps, TraversalMethods, TraversalSteps},
        traversal_value::TraversalValue,
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
use protocol::{request::Request, response::Response, ReturnValue, Value};
use rand::Rng;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};

use helixc::parser::helix_parser::{
    Expression, GraphStep, HelixParser, Source, StartNode, Statement, Step,
};

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
        let mut vars: HashMap<String, Vec<TraversalValue>> = HashMap::new();

        for query in ast.queries {
            for stmt in query.statements {
                match stmt {
                    Statement::Assignment(ass) => {
                        let value: Vec<TraversalValue> = match ass.value {
                            Expression::Traversal(tr) | Expression::Exists(tr) => {
                                // build traversal based on steps with traversal builder
                                // initialise from start node
                                // step through all steps and execute.
                                let start_nodes = match tr.start {
                                    StartNode::Vertex { types, ids }
                                    | StartNode::Edge { types, ids } => {
                                        let types = match types {
                                            Some(types) => types,
                                            None => vec![],
                                        };
                                        let ids = match ids {
                                            Some(ids) => ids,
                                            None => vec![],
                                        };
                                        ids.iter()
                                            .map(|id| match self.graph.storage.get_node(id) {
                                                Ok(n) => TraversalValue::SingleNode(n),
                                                Err(_) => TraversalValue::Empty,
                                            })
                                            .collect::<Vec<TraversalValue>>()
                                    },
                                    StartNode::Variable(var_name) => {
                                        match vars.get(&var_name) {
                                            Some(vals) => vals.clone(),
                                            None => return Err(HelixLiteError::from(format!("Variable: {} not found!", var_name))),
                                        }
                                    },
                                    _ => unreachable!(),
                                };
                                
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
                                        Step::Props(property_names) => {
                                            tr_builder.get_properties(&property_names);
                                        },
                                        // Step::Where(expression) => {
                                        //     tr_builder.filter(|val| {
                                        //         // Need to implement evaluation of expression against TraversalValue
                                        //         evaluate_expression(expression, val)
                                        //     });
                                        // },
                                        _ => unreachable!(),
                                    }
                                }

                                tr_builder.current_step
                            }
                            Expression::AddVertex(add_v) => {
                                let mut tr_builder =
                                    TraversalBuilder::new(&self.graph.storage, vec![]);
                                let label = match add_v.vertex_type {
                                    Some(l) => l,
                                    None => String::new(),
                                };
                                let props = match add_v.fields {
                                    Some(p) => p,
                                    None => props! {},
                                };
                                tr_builder.add_v(label.as_str(), props);
                                tr_builder.current_step
                            }
                            Expression::AddEdge(add_e) => {
                                let mut tr_builder =
                                    TraversalBuilder::new(&self.graph.storage, vec![]);
                                let label = match add_e.edge_type {
                                    Some(l) => l,
                                    None => String::new(),
                                };
                                let props = match add_e.fields {
                                    Some(p) => p,
                                    None => props! {},
                                };
                                tr_builder.add_e(
                                    label.as_str(),
                                    &add_e.connection.from_id,
                                    &add_e.connection.to_id,
                                    props,
                                );
                                tr_builder.current_step
                            }
                            _ => {
                                // insert variable to hashmap
                                let var = match ass.value {
                                    Expression::StringLiteral(value) => {
                                        TraversalValue::SingleValue((ass.variable.clone(), Value::String(value)))
                                    }
                                    Expression::NumberLiteral(value) => {
                                        TraversalValue::SingleValue((ass.variable.clone(), Value::Integer(value)))
                                    }
                                    Expression::BooleanLiteral(value) => {
                                        TraversalValue::SingleValue((ass.variable.clone(), Value::Boolean(value)))
                                    }
                                    _ => unreachable!(),
                                };
                                vec![var]
                            }
                        };

                        vars.insert(ass.variable, value);
                    }
                    Statement::AddVertex(add_v) => {
                        let mut tr_builder = TraversalBuilder::new(&self.graph.storage, vec![]);
                        let label = match add_v.vertex_type {
                            Some(l) => l,
                            None => String::new(),
                        };
                        let props = match add_v.fields {
                            Some(p) => p,
                            None => props! {},
                        };
                        tr_builder.add_v(label.as_str(), props);
                    }
                    Statement::AddEdge(add_e) => {
                        let mut tr_builder = TraversalBuilder::new(&self.graph.storage, vec![]);
                        let label = match add_e.edge_type {
                            Some(l) => l,
                            None => String::new(),
                        };
                        let props = match add_e.fields {
                            Some(p) => p,
                            None => props! {},
                        };
                        tr_builder.add_e(
                            label.as_str(),
                            &add_e.connection.from_id,
                            &add_e.connection.to_id,
                            props,
                        );
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

impl From<String> for HelixLiteError {
    fn from(error: String) -> Self {
        HelixLiteError::Default(error)
    }
}
