use std::any::Any;
use std::fmt::{Debug, Write};
use std::marker::PhantomData;

// Phantom types to track traversal state
#[derive(Debug)]
pub struct VertexState;
#[derive(Debug)]
pub struct EdgeState;
#[derive(Debug)]
pub struct NoState;


#[derive(Debug)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Boolean(b) => write!(f, "{}", b),
        }
    }
}

#[derive(Debug)]
pub enum ComparisonOperator {
    GT,
    LT,
    GTE,
    LTE,
    EQ,
    NEQ,
    Contains,
    StartsWith,
    EndsWith,
}

#[derive(Debug)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
}

#[derive(Debug)]
pub enum FilterCondition {
    // Property comparisons
    PropertyComparison {
        property: String,
        operator: ComparisonOperator,
        value: Value,
    },
    // Nested traversal comparison
    TraversalComparison {
        traversal: Vec<Box<dyn TraversalStepGenerator>>,
        operator: ComparisonOperator,
        value: Value,
    },
    // Logical combinations
    LogicalCombination {
        operator: LogicalOperator,
        conditions: Vec<FilterCondition>,
    },
    // Exists check
    PropertyExists {
        property: String,
    },
}

#[derive(Debug)]
pub enum TraversalStep<In, Out> {
    // Source steps
    V(PhantomData<(In, Out)>),
    E(PhantomData<(In, Out)>),
    AddV {
        label: String,
        _marker: PhantomData<(In, Out)>,
    },
    AddE {
        label: String,
        from_id: String,
        to_id: String,
        _marker: PhantomData<(In, Out)>,
    },

    // Traversal steps
    Out {
        label: String,
        _marker: PhantomData<(In, Out)>,
    },
    OutE {
        label: String,
        _marker: PhantomData<(In, Out)>,
    },
    In {
        label: String,
        _marker: PhantomData<(In, Out)>,
    },
    InE {
        label: String,
        _marker: PhantomData<(In, Out)>,
    },

    // Method Steps
    Count {
        _marker: PhantomData<(In, Out)>,
    },
    Range {
        start: usize,
        end: usize,
        _marker: PhantomData<(In, Out)>,
    },
    Filter {
        condition: FilterCondition,
        _marker: PhantomData<(In, Out)>,
    },
}

/// ## Traversal Generator
/// Builds up a traversal based on what is in the AST.
///
/// It uses phantom data allow specific traversal transitions to ensure incompatible traversal steps (e.g. `Edge->OutE`) are not allowed.
///
#[derive(Debug)]
pub struct TraversalGenerator<CurrentState> {
    function_identifier: String,
    steps: Vec<Box<dyn TraversalStepGenerator>>,
    _marker: PhantomData<CurrentState>,
}

pub trait TraversalStepGenerator: Debug {
    fn generate_code(&self, f: &mut String) -> std::fmt::Result;
}

impl<In: Debug, Out: Debug> TraversalStepGenerator for TraversalStep<In, Out> {
    fn generate_code(&self, f: &mut String) -> std::fmt::Result {
        match self {
            TraversalStep::V(_) => writeln!(f, "    traversal.v();"),
            TraversalStep::E(_) => writeln!(f, "    traversal.e();"),
            TraversalStep::AddV { label, .. } => {
                writeln!(f, "    traversal.add_v(\"{}\");", label)
            }
            TraversalStep::AddE {
                label,
                from_id,
                to_id,
                ..
            } => {
                writeln!(
                    f,
                    "    traversal.add_e(\"{}\", \"{}\", \"{}\");",
                    label, from_id, to_id
                )
            }
            TraversalStep::Out { label, .. } => {
                writeln!(f, "    traversal.out(\"{}\");", label)
            }
            TraversalStep::OutE { label, .. } => {
                writeln!(f, "    traversal.out_e(\"{}\");", label)
            }
            TraversalStep::In { label, .. } => {
                writeln!(f, "    traversal.in_(\"{}\");", label)
            }
            TraversalStep::InE { label, .. } => {
                writeln!(f, "    traversal.in_e(\"{}\");", label)
            }
            TraversalStep::Count { .. } => writeln!(f, "    traversal.count();"),
            TraversalStep::Range { start, end, .. } => {
                writeln!(f, "    traversal.range({}, {});", start, end)
            },
            TraversalStep::Filter { condition, .. } => {
                generate_filter_condition(f, condition)
            }
           
        }
    }
}





impl<T> TraversalGenerator<T> {
    pub fn generate_code(&self) -> Result<String, std::fmt::Error> {
        let mut code = String::new();

        writeln!(
            code,
            "pub fn {}(input: &HandlerInput, response: &mut Response) ->  Result<(), RouterError> {{", self.function_identifier
        )?;

        writeln!(code, "    let storage = &input.graph.storage;")?;
        writeln!(
            code,
            "    let mut traversal = TraversalBuilder::new(vec![]);"
        )?;

        for step  in &self.steps{
            step.generate_code(&mut code)?;
        }

        writeln!(
            code,
            "    response.body = input.graph.result_to_json(&traversal);"
        )?;
        writeln!(code, "    Ok(())")?;
        writeln!(code, "}}")?;

        Ok(code)
    }
}

impl TraversalGenerator<NoState> {
    pub fn new(function_identifier: &str) -> Self {
        Self {
            function_identifier: function_identifier.to_string(),
            steps: Vec::new(),
            _marker: PhantomData,
        }
    }

    // Source steps that start a traversal
    pub fn v(mut self) -> TraversalGenerator<VertexState> {
        self.steps
            .push(Box::new(TraversalStep::<NoState, VertexState>::V(
                PhantomData,
            )));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn e(mut self) -> TraversalGenerator<EdgeState> {
        self.steps
            .push(Box::new(TraversalStep::<NoState, EdgeState>::E(
                PhantomData,
            )));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl TraversalGenerator<VertexState> {
    pub fn out(mut self, label: &str) -> TraversalGenerator<VertexState> {
        self.steps
            .push(Box::new(TraversalStep::<NoState, VertexState>::Out {
                label: label.to_string(),
                _marker: PhantomData,
            }));
        self
    }

    pub fn out_e(mut self, label: &str) -> TraversalGenerator<EdgeState> {
        self.steps
            .push(Box::new(TraversalStep::<NoState, EdgeState>::OutE {
                label: label.to_string(),
                _marker: PhantomData,
            }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn in_(mut self, label: &str) -> TraversalGenerator<VertexState> {
        self.steps
            .push(Box::new(TraversalStep::<NoState, VertexState>::In {
                label: label.to_string(),
                _marker: PhantomData,
            }));
        self
    }

    pub fn in_e(mut self, label: &str) -> TraversalGenerator<EdgeState> {
        self.steps
            .push(Box::new(TraversalStep::<NoState, EdgeState>::InE {
                label: label.to_string(),
                _marker: PhantomData,
            }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn where_gt(mut self, property: &str, value: Value) -> TraversalGenerator<VertexState> {
        self.steps.push(Box::new(TraversalStep::<NoState, VertexState>::Filter {
            condition: FilterCondition::PropertyComparison {
                property: property.to_string(),
                operator: ComparisonOperator::GT,
                value,
            },
            _marker: PhantomData,
        }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn where_lt(mut self, property: &str, value: Value) -> TraversalGenerator<VertexState> {
        self.steps.push(Box::new(TraversalStep::<NoState, VertexState>::Filter {
            condition: FilterCondition::PropertyComparison {
                property: property.to_string(),
                operator: ComparisonOperator::LT,
                value,
            },
            _marker: PhantomData,
        }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn where_gte(mut self, property: &str, value: Value) -> Self {
        self.steps.push(Box::new(TraversalStep::<NoState, VertexState>::Filter {
            condition: FilterCondition::PropertyComparison {
                property: property.to_string(),
                operator: ComparisonOperator::GTE,
                value,
            },
            _marker: PhantomData,
        }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn where_lte(mut self, property: &str, value: Value) -> Self {
        self.steps.push(Box::new(TraversalStep::<NoState, VertexState>::Filter {
            condition: FilterCondition::PropertyComparison {
                property: property.to_string(),
                operator: ComparisonOperator::LTE,
                value,
            },
            _marker: PhantomData,
        }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn where_eq(mut self, property: &str, value: Value) -> Self {
        self.steps.push(Box::new(TraversalStep::<NoState, VertexState>::Filter {
            condition: FilterCondition::PropertyComparison {
                property: property.to_string(),
                operator: ComparisonOperator::EQ,
                value,
            },
            _marker: PhantomData,
        }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn where_neq(mut self, property: &str, value: Value) -> Self {
        self.steps.push(Box::new(TraversalStep::<NoState, VertexState>::Filter {
            condition: FilterCondition::PropertyComparison {
                property: property.to_string(),
                operator: ComparisonOperator::NEQ,
                value,
            },
            _marker: PhantomData,
        }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn where_contains(mut self, property: &str, value: Value) -> Self {
        self.steps.push(Box::new(TraversalStep::<NoState, VertexState>::Filter {
            condition: FilterCondition::PropertyComparison {
                property: property.to_string(),
                operator: ComparisonOperator::Contains,
                value,
            },
            _marker: PhantomData,
        }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn where_starts_with(mut self, property: &str, value: Value) -> Self {
        self.steps.push(Box::new(TraversalStep::<NoState, VertexState>::Filter {
            condition: FilterCondition::PropertyComparison {
                property: property.to_string(),
                operator: ComparisonOperator::StartsWith,
                value,
            },
            _marker: PhantomData,
        }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn where_ends_with(mut self, property: &str, value: Value) -> Self {
        self.steps.push(Box::new(TraversalStep::<NoState, VertexState>::Filter {
            condition: FilterCondition::PropertyComparison {
                property: property.to_string(),
                operator: ComparisonOperator::EndsWith,
                value,
            },
            _marker: PhantomData,
        }));
        TraversalGenerator {
            function_identifier: self.function_identifier,
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}


// Helper trait for value comparison code generation
trait ValueComparison {
    fn generate_comparison_code(&self, operator: &ComparisonOperator, value: &Value) -> String;
}

impl ValueComparison for &str {
    fn generate_comparison_code(&self, operator: &ComparisonOperator, value: &Value) -> String {
        match operator {
            ComparisonOperator::GT => format!("*{} > {}", self, value),
            ComparisonOperator::LT => format!("*{} < {}", self, value),
            ComparisonOperator::GTE => format!("*{} >= {}", self, value),
            ComparisonOperator::LTE => format!("*{} <= {}", self, value),
            ComparisonOperator::EQ => format!("*{} == {}", self, value),
            ComparisonOperator::NEQ => format!("*{} != {}", self, value),
            ComparisonOperator::Contains => format!("{}.contains(&{})", self, value),
            ComparisonOperator::StartsWith => format!("{}.starts_with(&{})", self, value),
            ComparisonOperator::EndsWith => format!("{}.ends_with(&{})", self, value),
        }
    }
}


fn generate_filter_condition(f: &mut String, condition: &FilterCondition) -> std::fmt::Result {
    match condition {
        FilterCondition::PropertyComparison { property, operator, value } => {
            writeln!(f, "    traversal.filter(|val| {{")?;
            writeln!(f, "        match val {{")?;
            writeln!(f, "            TraversalValue::SingleNode(node) => {{")?;
            writeln!(f, "                if let Some(prop_val) = node.properties.get(\"{}\") {{", property)?;
            writeln!(f, "                    match prop_val {{")?;
            generate_value_match(f, operator, value)?;
            writeln!(f, "                        _ => false,")?;
            writeln!(f, "                    }}")?;
            writeln!(f, "                }} else {{ false }}")?;
            writeln!(f, "            }},")?;
            writeln!(f, "            TraversalValue::SingleEdge(edge) => {{")?;
            writeln!(f, "                if let Some(prop_val) = edge.properties.get(\"{}\") {{", property)?;
            writeln!(f, "                    match prop_val {{")?;
            generate_value_match(f, operator, value)?;
            writeln!(f, "                        _ => false,")?;
            writeln!(f, "                    }}")?;
            writeln!(f, "                }} else {{ false }}")?;
            writeln!(f, "            }},")?;
            writeln!(f, "            _ => false,")?;
            writeln!(f, "        }}")?;
            writeln!(f, "    }});")?;
        },
        FilterCondition::TraversalComparison { traversal, operator, value } => {
            writeln!(f, "    traversal.filter(|val| {{")?;
            writeln!(f, "        let mut sub_traversal = TraversalBuilder::new(vec![val.clone()]);")?;
            for step in traversal {
                step.generate_code(f)?;
            }
            writeln!(f, "        let result = sub_traversal.count();")?;
            writeln!(f, "        {}", generate_numeric_comparison("result", operator, value))?;
            writeln!(f, "    }});")?;
        },
        FilterCondition::LogicalCombination { operator, conditions } => {
            writeln!(f, "    traversal.filter(|val| {{")?;
            match operator {
                LogicalOperator::And => {
                    for (i, condition) in conditions.iter().enumerate() {
                        if i > 0 {
                            write!(f, " && ")?;
                        }
                        generate_filter_condition(f, condition)?;
                    }
                },
                LogicalOperator::Or => {
                    for (i, condition) in conditions.iter().enumerate() {
                        if i > 0 {
                            write!(f, " || ")?;
                        }
                        generate_filter_condition(f, condition)?;
                    }
                },
                LogicalOperator::Not => {
                    write!(f, "!")?;
                    generate_filter_condition(f, &conditions[0])?;
                }
            }
            writeln!(f, "    }});")?;
        },
        FilterCondition::PropertyExists { property } => {
            writeln!(f, "    traversal.filter(|val| {{")?;
            writeln!(f, "        match val {{")?;
            writeln!(f, "            TraversalValue::SingleNode(node) => node.properties.contains_key(\"{}\"),", property)?;
            writeln!(f, "            TraversalValue::SingleEdge(edge) => edge.properties.contains_key(\"{}\"),", property)?;
            writeln!(f, "            _ => false,")?;
            writeln!(f, "        }}")?;
            writeln!(f, "    }});")?;
        }
    }
    Ok(())
}

fn generate_value_match(f: &mut String, operator: &ComparisonOperator, value: &Value) -> std::fmt::Result {
    match value {
        Value::Integer(_) => {
            writeln!(f, "                        Value::Integer(val) => {},", "val".generate_comparison_code(operator, value))?;
        },
        Value::Float(_) => {
            writeln!(f, "                        Value::Float(val) => {},", "val".generate_comparison_code(operator, value))?;
        },
        Value::String(_) => {
            writeln!(f, "                        Value::String(val) => {},", "val".generate_comparison_code(operator, value))?;
        },
        Value::Boolean(_) => {
            writeln!(f, "                        Value::Boolean(val) => {},", "val".generate_comparison_code(operator, value))?;
        },
    }
    Ok(())
}

fn generate_numeric_comparison(var_name: &str, operator: &ComparisonOperator, value: &Value) -> String {
    match operator {
        ComparisonOperator::GT => format!("{} > {}", var_name, value),
        ComparisonOperator::LT => format!("{} < {}", var_name, value),
        ComparisonOperator::GTE => format!("{} >= {}", var_name, value),
        ComparisonOperator::LTE => format!("{} <= {}", var_name, value),
        ComparisonOperator::EQ => format!("{} == {}", var_name, value),
        ComparisonOperator::NEQ => format!("{} != {}", var_name, value),
        _ => panic!("Invalid operator for numeric comparison"),
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_safe_traversal() {
        let generator = TraversalGenerator::new("test_function")
            .v()
            .out("knows")
            .in_("follows")
            .out_e("likes");

        let code = generator.generate_code().unwrap();

        assert!(code.contains("pub fn test_function("));
        assert!(code.contains("traversal.v(storage);"));
        assert!(code.contains("traversal.out(\"knows\");"));
        assert!(code.contains("traversal.in_(\"follows\");"));
        assert!(code.contains("traversal.out_e(\"likes\");"));
    }
}
