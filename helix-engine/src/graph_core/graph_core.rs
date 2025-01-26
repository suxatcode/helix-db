use crate::storage_core::storage_methods::StorageMethods;
use crate::types::GraphError;
use crate::{props, HelixGraphStorage};
use std::collections::HashMap;
use std::ops::Deref;
use std::str;
use std::sync::{Arc, RwLock};

use super::traversal::TraversalBuilder;
use super::traversal_steps::{SourceTraversalSteps, TraversalMethods, TraversalSteps};
use helixc::parser::helix_parser::{
    BooleanOp, Expression, GraphStep, HelixParser, IdType, Source, StartNode, Statement, Step,
    Traversal,
};
use protocol::traversal_value::TraversalValue;
use protocol::{value::Value, Node, ReturnValue};
use serde_json::json;

#[derive(Debug)]
pub enum QueryInput {
    StringValue { value: String },
    IntegerValue { value: i32 },
    FloatValue { value: f64 },
    BooleanValue { value: bool },
}

pub struct HelixGraphEngine {
    pub storage: Arc<HelixGraphStorage>,
}

impl HelixGraphEngine {
    pub fn new(path: &str) -> Result<HelixGraphEngine, GraphError> {
        let storage = match HelixGraphStorage::new(path) {
            Ok(db) => Arc::new(db),
            Err(err) => return Err(err),
        };
        Ok(Self { storage })
    }

    pub fn print_result_as_json(&self, traversal: &TraversalBuilder) {
        let current_step = &traversal.current_step;
        let json_result = json!(current_step);
        println!("{}", json_result.to_string());
    }

    pub fn print_result_as_pretty_json(&self, traversal: &TraversalBuilder) {
        let current_step = &traversal.current_step;
        let json_result = json!(current_step);
        println!("{}", serde_json::to_string_pretty(&json_result).unwrap());
    }

    /// implement error for this function
    pub fn result_to_json(&self, traversal: &TraversalBuilder) -> Vec<u8> {
        let current_step = &traversal.current_step;
        let mut json_string = serde_json::to_string(current_step).unwrap();
        json_string.push_str("\n");
        json_string.into_bytes()
    }

    pub fn result_to_json_string(&self, traversal: &TraversalBuilder) -> String {
        let current_step = &traversal.current_step;
        let mut json_string = serde_json::to_string(current_step).unwrap();
        json_string.push_str("\n");
        json_string
    }

    pub fn query(&self, query: String, params: Vec<QueryInput>) -> Result<String, GraphError> {
        let ast: Source = match HelixParser::parse_source(query.as_str()) {
            Ok(src) => src,
            Err(err) => return Err(GraphError::from(err)),
        };
        let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        let vars: Arc<RwLock<HashMap<String, ReturnValue>>> = Arc::new(RwLock::new(HashMap::new()));
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
                                    Arc::clone(&self.storage),
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
                                    Arc::clone(&self.storage),
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
                                    &Self::id_type_to_id(
                                        add_e.connection.from_id,
                                        Arc::clone(&vars),
                                    )?,
                                    &Self::id_type_to_id(
                                        add_e.connection.to_id,
                                        Arc::clone(&vars),
                                    )?,
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
                            TraversalBuilder::new(Arc::clone(&self.storage), TraversalValue::Empty);
                        let label = add_v.vertex_type.unwrap_or_default();
                        let props = add_v.fields.unwrap_or_default();

                        tr_builder.add_v(label.as_str(), props);
                    }
                    Statement::AddEdge(add_e) => {
                        let mut tr_builder =
                            TraversalBuilder::new(Arc::clone(&self.storage), TraversalValue::Empty);

                        let label = add_e.edge_type.unwrap_or_default();
                        let props = add_e.fields.unwrap_or_default();

                        tr_builder.add_e(
                            label.as_str(),
                            &Self::id_type_to_id(add_e.connection.from_id, Arc::clone(&vars))?,
                            &Self::id_type_to_id(add_e.connection.to_id, Arc::clone(&vars))?,
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
                                return Err(GraphError::from("Return value must be a variable!"));
                            }
                        };
                        if let Some(val) = vars.read().unwrap().get(&var_name) {
                            return_vals.insert(var_name, val.clone()); // fix clone
                        }
                    }
                    _ => {
                        return Err(GraphError::from("Return value must be a variable!"));
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
    ) -> Result<ReturnValue, GraphError> {
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
                    0 => match types.len() {
                        0 => TraversalValue::NodeArray(Arc::clone(&self.storage).get_all_nodes()?),
                        _ => TraversalValue::NodeArray(
                            Arc::clone(&self.storage).get_nodes_by_types(&types)?,
                        ),
                    },
                    _ => TraversalValue::NodeArray(
                        ids.iter()
                            .map(|id| match Arc::clone(&self.storage).get_node(id) {
                                Ok(n) => Ok(n),
                                Err(_) => {
                                    return Err(GraphError::from(format!(
                                        "Node with id: {} not found!",
                                        id
                                    )))
                                }
                            })
                            .collect::<Result<Vec<Node>, GraphError>>()?,
                    ),
                }
            }
            StartNode::Variable(var_name) => match vars.read().unwrap().get(&var_name) {
                Some(vals) => match vals.clone() {
                    ReturnValue::TraversalValues(vals) => vals,
                    _ => unreachable!(),
                },
                None => {
                    return Err(GraphError::from(format!(
                        "Variable: {} not found!",
                        var_name
                    )))
                }
            },
            StartNode::Anonymous => anon_start,
            _ => unreachable!(),
        };

        let mut tr_builder = TraversalBuilder::new(Arc::clone(&self.storage), start_nodes);

        for step in &tr.steps {
            match step {
                Step::Vertex(graph_step) => match graph_step {
                    GraphStep::Out(labels) => match labels {
                        Some(l) => {
                            if l.len() > 1 {
                                return Err(GraphError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
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
                                return Err(GraphError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
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
                                return Err(GraphError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
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
                                return Err(GraphError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
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
                                return Err(GraphError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
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
                                return Err(GraphError::from("Cannot use more than 1 label yet! This feature will be coming soon."));
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
                    assert!(property_names.len() > 0, "Property names must be provided!");
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
                                    return Err(GraphError::from(
                                        format!("Exists clause must follow a traversal step! Got step: {:?}", anon_tr.start ),
                                    ));
                                }
                            },
                            _ => {
                                return Err(GraphError::from("Where clause must start with an anonymous traversal or exists query!"));
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
                                    return Err(GraphError::from(
                                        "Exists clause must follow a traversal step!",
                                    ));
                                }
                            },
                            _ => {
                                return Err(GraphError::from("Where clause must start with an anonymous traversal or exists query!"));
                            }
                        },
                        _ => {
                            return Err(GraphError::from("Where clause must start with an anonymous traversal or exists query!"));
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
                                return Err(GraphError::from(
                                    "Exists clause must follow a traversal step!",
                                ));
                            }
                        },
                        _ => {
                            return Err(GraphError::from("Where clause must start with an anonymous traversal or exists query!"));
                        }
                    }
                }
                Step::BooleanOperation(op) => {
                    // let previous_step = tr.steps[index - 1].clone();
                    // match previous_step {
                    //     Step::Count => {}
                    //     Step::Props(_) => {}
                    //     _ => {
                    //         return Err(GraphError::from(format!(
                    //             "Boolean operation must follow a traversal step! Got step: {:?}",
                    //             previous_step
                    //         )));
                    //     }
                    // };

                    match tr_builder.current_step {
                        TraversalValue::Count(count) => {
                            return Ok(ReturnValue::Boolean(Self::manage_int_bool_exp(
                                op,
                                count.value() as i32,
                            )))
                        }
                        TraversalValue::ValueArray(ref vals) => {
                            let mut res = Vec::with_capacity(vals.len());
                            for (_, val) in vals {
                                match val {
                                    Value::Integer(val) => {
                                        res.push(Self::manage_int_bool_exp(op, val.clone()));
                                    }
                                    Value::Float(val) => {
                                        res.push(Self::manage_float_bool_exp(op, val.clone()));
                                    }
                                    _ => {
                                        return Err(GraphError::from(
                                            "Expression should resolve to a number!",
                                        ));
                                    }
                                }
                            }
                            return Ok(ReturnValue::Boolean(res.iter().all(|&x| x)));
                        }
                        _ => {
                            return Err(GraphError::from(
                                format!("Boolean operation must follow a count or numerical property step! Got step: {:?} for traversal {:?}", tr_builder.current_step, step),
                            ));
                        }
                    };
                }
                _ => unreachable!(),
            }
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

    fn id_type_to_id(
        id_type: IdType,
        vars: Arc<RwLock<HashMap<String, ReturnValue>>>,
    ) -> Result<String, GraphError> {
        match id_type {
            IdType::Literal(s) => Ok(s),
            IdType::Identifier(s) => {
                let reader = vars.read().unwrap();
                let vals = reader.get(&s).unwrap();
                match vals {
                    ReturnValue::TraversalValues(tr) => {
                        match tr {
                            TraversalValue::NodeArray(arr) => {
                                if arr.len() != 1 {
                                    // throw err
                                    return Err(GraphError::from(format!(
                                        "Node array too long, expected length 1 but got length {}",
                                        arr.len()
                                    )));
                                };
                                // get first and get id
                                let node = arr.first().unwrap();
                                Ok(node.id.clone())
                            }
                            TraversalValue::EdgeArray(arr) => {
                                if arr.len() != 1 {
                                    // throw error
                                    return Err(GraphError::from(format!(
                                        "Edge array too long, expected length 1 but got length {}",
                                        arr.len()
                                    )));
                                };
                                let edge = arr.first().unwrap();
                                Ok(edge.id.clone().deref().to_string()) // change
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}
