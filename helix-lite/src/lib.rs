use bindings::js_binding::HelixJS;
use chrono::Utc;
use helix_engine::{
    graph_core::{
        graph_core::HelixGraphEngine,
        traversal::TraversalBuilder,
        traversal_steps::{SourceTraversalSteps, TraversalMethods, TraversalSteps},
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
use protocol::{
    count::Count, request::Request, response::Response, traversal_value::TraversalValue, Node,
    ReturnValue, Value,
};
use rand::Rng;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock}, time::Instant,
};

use helixc::{
    generator::query_gen::TraversalStep,
    parser::helix_parser::{
        BooleanOp, Expression, GraphStep, HelixParser, Source, StartNode, Statement, Step,
        Traversal,
    },
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
        let ast: Source = HelixParser::parse_source(query.as_str()).unwrap();
        let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        let mut vars: Arc<RwLock<HashMap<String, ReturnValue>>> =
            Arc::new(RwLock::new(HashMap::new()));
        // let mut results = Vec::with_capacity(return_vals.len());


        for query in ast.queries {
            for stmt in query.statements {
                match stmt {
                    Statement::Assignment(ass) => {
                        let value: ReturnValue = match ass.value {
                            Expression::Traversal(tr) | Expression::Exists(tr) => {
                                // build traversal based on steps with traversal builder
                                // initialise from start node
                                // step through all steps and execute.
                                self.evaluate_traversal(
                                    tr,
                                    Arc::clone(&vars),
                                    TraversalValue::Empty,
                                )?
                            }
                            Expression::AddVertex(add_v) => {
                                let mut tr_builder = TraversalBuilder::new(
                                    &self.graph.storage,
                                    TraversalValue::Empty,
                                );
                                let label = match add_v.vertex_type {
                                    Some(l) => l,
                                    None => String::new(),
                                };
                                let props = match add_v.fields {
                                    Some(p) => p,
                                    None => props! {},
                                };
                                tr_builder.add_v(label.as_str(), props);
                                ReturnValue::TraversalValues(tr_builder.current_step)
                            }
                            Expression::AddEdge(add_e) => {
                                let mut tr_builder = TraversalBuilder::new(
                                    &self.graph.storage,
                                    TraversalValue::Empty,
                                );
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
                                ReturnValue::TraversalValues(tr_builder.current_step)
                            }
                            _ => {
                                // insert variable to hashmap
                                let var =
                                    match ass.value {
                                        Expression::StringLiteral(value) => TraversalValue::from((
                                            ass.variable.clone(),
                                            Value::String(value),
                                        )),
                                        Expression::IntegerLiteral(value) => TraversalValue::from(
                                            (ass.variable.clone(), Value::Integer(value)),
                                        ),
                                        Expression::FloatLiteral(value) => TraversalValue::from((
                                            ass.variable.clone(),
                                            Value::Float(value),
                                        )),
                                        Expression::BooleanLiteral(value) => TraversalValue::from(
                                            (ass.variable.clone(), Value::Boolean(value)),
                                        ),
                                        _ => unreachable!(),
                                    };
                                ReturnValue::TraversalValues(var)
                            }
                        };

                        vars.write().unwrap().insert(ass.variable, value);
                    }
                    Statement::AddVertex(add_v) => {
                        let mut tr_builder =
                            TraversalBuilder::new(&self.graph.storage, TraversalValue::Empty);
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
                        let mut tr_builder =
                            TraversalBuilder::new(&self.graph.storage, TraversalValue::Empty);
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
            for return_value in query.return_values {
                match return_value {
                    Expression::Identifier(var_name) => {
                        if let Some(val) = vars.read().unwrap().get(&var_name) {
                            return_vals.insert(var_name, val.clone()); // fix clone
                        }
                    }
                    Expression::Traversal(tr) => {
                        let var_name = match tr.start {
                            StartNode::Variable(var_name) => var_name,
                            _ => {
                                return Err(HelixLiteError::from(
                                    "Return value must be a variable!",
                                ));
                            }
                        };
                        if let Some(val) = vars.read().unwrap().get(&var_name) {
                            return_vals.insert(var_name, val.clone()); // fix clone
                        }
                    }
                    _ => {
                        return Err(HelixLiteError::from("Return value must be a variable!"));
                    }
                }
            }
        }


        let json_string = serde_json::to_string_pretty(&return_vals).unwrap();
        Ok(json_string)
    }

    fn evaluate_traversal(
        &self,
        tr: Box<Traversal>,
        vars: Arc<RwLock<HashMap<String, ReturnValue>>>,
        anon_start: TraversalValue,
    ) -> Result<ReturnValue, HelixLiteError> {
        let start_nodes: TraversalValue = match tr.start {
            StartNode::Vertex { types, ids } | StartNode::Edge { types, ids } => {
                let types = match types {
                    Some(types) => types,
                    None => vec![],
                };
                let ids = match ids {
                    Some(ids) => ids,
                    None => vec![],
                };

                match ids.len() {
                    0 => TraversalValue::NodeArray(self.graph.storage.get_all_nodes()?),
                    _ => TraversalValue::NodeArray(
                        ids.iter()
                            .map(|id| match self.graph.storage.get_node(id) {
                                Ok(n) => Ok(n),
                                Err(_) => {
                                    return Err(HelixLiteError::from(format!(
                                        "Node with id: {} not found!",
                                        id
                                    )))
                                }
                            })
                            .collect::<Result<Vec<Node>, HelixLiteError>>()?,
                    ),
                }
            }
            StartNode::Variable(var_name) => match vars.read().unwrap().get(&var_name) {
                Some(vals) => match vals.clone() {
                    ReturnValue::TraversalValues(vals) => vals,
                    _ => unreachable!(),
                },
                None => {
                    return Err(HelixLiteError::from(format!(
                        "Variable: {} not found!",
                        var_name
                    )))
                }
            },
            StartNode::Anonymous => anon_start,
            _ => unreachable!(),
        };

        let mut tr_builder = TraversalBuilder::new(&self.graph.storage, start_nodes);
        let mut index = 0;

        for step in &tr.steps {
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
                    tr_builder.get_properties(property_names);
                }
                Step::Where(expression) => {
                    match &**expression {
                        Expression::Traversal(anon_tr) => match anon_tr.start {
                            StartNode::Anonymous => match tr_builder.current_step {
                                TraversalValue::NodeArray(_) => {
                                    tr_builder.filter_nodes(|val| {
                                        match self.evaluate_traversal(
                                            anon_tr.clone(),
                                            Arc::clone(&vars),
                                            TraversalValue::from(val),
                                        )? {
                                            ReturnValue::Boolean(val) => Ok(val),
                                            _ => {
                                                return Err(GraphError::from(
                                                    "Where clause must evaluate to a boolean!",
                                                ));
                                            }
                                        }
                                    });
                                }
                                TraversalValue::EdgeArray(_) => {
                                    tr_builder.filter_edges(|val| {
                                        match self.evaluate_traversal(
                                            anon_tr.clone(),
                                            Arc::clone(&vars),
                                            TraversalValue::from(val),
                                        )? {
                                            ReturnValue::Boolean(res) => Ok(res),
                                            _ => {
                                                return Err(GraphError::from(
                                                    "Where clause must evaluate to a boolean!",
                                                ));
                                            }
                                        }
                                    });
                                }
                                _ => {
                                    return Err(HelixLiteError::from(
                                        "Exists clause must follow a traversal step!",
                                    ));
                                }
                            },
                            _ => {
                                return Err(HelixLiteError::from("Where clause must start with an anonymous traversal or exists query!"));
                            }
                        },

                        Expression::Exists(anon_tr) => match anon_tr.start {
                            StartNode::Anonymous => match tr_builder.current_step {
                                TraversalValue::NodeArray(_) => {
                                    tr_builder.filter_nodes(|val| {
                                        match self.evaluate_traversal(
                                            anon_tr.clone(),
                                            Arc::clone(&vars),
                                            TraversalValue::from(val),
                                        )? {
                                            ReturnValue::Boolean(val) => Ok(val),
                                            _ => {
                                                return Err(GraphError::from(
                                                    "Where clause must evaluate to a boolean!",
                                                ));
                                            }
                                        }
                                    });
                                }
                                TraversalValue::EdgeArray(_) => {
                                    tr_builder.filter_edges(|val| {
                                        match self.evaluate_traversal(
                                            anon_tr.clone(),
                                            Arc::clone(&vars),
                                            TraversalValue::from(val),
                                        )? {
                                            ReturnValue::Boolean(res) => Ok(res),
                                            _ => {
                                                return Err(GraphError::from(
                                                    "Where clause must evaluate to a boolean!",
                                                ));
                                            }
                                        }
                                    });
                                }
                                _ => {
                                    return Err(HelixLiteError::from(
                                        "Exists clause must follow a traversal step!",
                                    ));
                                }
                            },
                            _ => {
                                return Err(HelixLiteError::from("Where clause must start with an anonymous traversal or exists query!"));
                            }
                        },
                        _ => {
                            return Err(HelixLiteError::from("Where clause must start with an anonymous traversal or exists query!"));
                        }
                    }
                }
                Step::Exists(expression) => {
                    match expression.start {
                        StartNode::Anonymous => match tr_builder.current_step {
                            TraversalValue::NodeArray(_) => {
                                tr_builder.filter_nodes(|val| {
                                    match self.evaluate_traversal(
                                        expression.clone(),
                                        Arc::clone(&vars),
                                        TraversalValue::from(val),
                                    )? {
                                        ReturnValue::Boolean(val) => Ok(val),
                                        _ => {
                                            return Err(GraphError::from(
                                                "Where clause must evaluate to a boolean!",
                                            ));
                                        }
                                    }
                                });
                            }
                            TraversalValue::EdgeArray(_) => {
                                tr_builder.filter_edges(|val| {
                                    match self.evaluate_traversal(
                                        expression.clone(),
                                        Arc::clone(&vars),
                                        TraversalValue::from(val),
                                    )? {
                                        ReturnValue::Boolean(res) => Ok(res),
                                        _ => {
                                            return Err(GraphError::from(
                                                "Where clause must evaluate to a boolean!",
                                            ));
                                        }
                                    }
                                });
                            }
                            _ => {
                                return Err(HelixLiteError::from(
                                    "Exists clause must follow a traversal step!",
                                ));
                            }
                        },
                        _ => {
                            return Err(HelixLiteError::from("Where clause must start with an anonymous traversal or exists query!"));
                        }
                    }
                }
                Step::BooleanOperation(op) => {
                    if index == 0 {
                        return Err(HelixLiteError::from(
                            "Boolean operation must follow a traversal step!",
                        ));
                    }
                    let previous_step = tr.steps[index - 1].clone();
                    match previous_step {
                        Step::Count => {}
                        Step::Props(_) => {}
                        _ => {
                            return Err(HelixLiteError::from(
                                "Boolean operation must follow a traversal step!",
                            ));
                        }
                    };

                    match tr_builder.current_step {
                        TraversalValue::Count(count) => {
                            return Ok(ReturnValue::Boolean(Self::manage_int_bool_exp(
                                op,
                                count.value() as i32,
                            )))
                        }
                        // TraversalValue::ValueArray(vals) => {
                        //     let mut res = Vec::with_capacity(vals.len());
                        //     for (_, val) in vals {
                        //         match val {
                        //             Value::Integer(val) => {
                        //                 res.push(Self::manage_int_bool_exp(op, *val));
                        //             }
                        //             Value::Float(val) => {
                        //                 res.push(Self::manage_float_bool_exp(op, *val));
                        //             }
                        //             _ => {
                        //                 return Err(HelixLiteError::from(
                        //                     "Expression should resolve to a number!",
                        //                 ));
                        //             }
                        //         }
                        //     }
                        // }
                        _ => {
                            return Err(HelixLiteError::from(
                                "Boolean operation must follow a traversal step!",
                            ));
                        }
                    };
                }
                _ => unreachable!(),
            }
            index += 1;
        }

        Ok(ReturnValue::TraversalValues(tr_builder.current_step))
    }

    fn manage_float_bool_exp(op: &BooleanOp, fl: f64) -> bool {
        match op {
            BooleanOp::GreaterThan(expr) => match **expr {
                Expression::FloatLiteral(val) => {
                    return fl > val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::GreaterThanOrEqual(expr) => match **expr {
                Expression::FloatLiteral(val) => {
                    return fl >= val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::LessThan(expr) => match **expr {
                Expression::FloatLiteral(val) => {
                    return fl < val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::LessThanOrEqual(expr) => match **expr {
                Expression::FloatLiteral(val) => {
                    return fl <= val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::Equal(expr) => match **expr {
                Expression::FloatLiteral(val) => {
                    return fl == val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::NotEqual(expr) => match **expr {
                Expression::FloatLiteral(val) => {
                    return fl != val;
                }
                _ => {
                    return false;
                }
            },
            _ => {
                return false;
            }
        };
    }

    fn manage_int_bool_exp(op: &BooleanOp, i: i32) -> bool {
        match op {
            BooleanOp::GreaterThan(expr) => match **expr {
                Expression::IntegerLiteral(val) => {
                    return i > val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::GreaterThanOrEqual(expr) => match **expr {
                Expression::IntegerLiteral(val) => {
                    return i >= val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::LessThan(expr) => match **expr {
                Expression::IntegerLiteral(val) => {
                    return i < val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::LessThanOrEqual(expr) => match **expr {
                Expression::IntegerLiteral(val) => {
                    return i <= val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::Equal(expr) => match **expr {
                Expression::IntegerLiteral(val) => {
                    return i == val;
                }
                _ => {
                    return false;
                }
            },
            BooleanOp::NotEqual(expr) => match **expr {
                Expression::IntegerLiteral(val) => {
                    return i != val;
                }
                _ => {
                    return false;
                }
            },
            _ => {
                return false;
            }
        };
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

impl From<HelixLiteError> for GraphError {
    fn from(error: HelixLiteError) -> Self {
        GraphError::from(error.to_string())
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
