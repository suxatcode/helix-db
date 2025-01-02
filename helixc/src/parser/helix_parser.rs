use super::parser_methods::ParserError;
use pest::{iterators::Pair, Parser as PestParser};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar.pest"]

pub struct HelixParser;

// Schema-related structs
#[derive(Debug)]
pub struct Source {
    pub node_schemas: Vec<NodeSchema>,
    pub edge_schemas: Vec<EdgeSchema>,
    pub queries: Vec<Query>,
}

#[derive(Debug)]
pub struct NodeSchema {
    pub name: String,
    pub fields: Vec<Field>,
}

#[derive(Debug)]
pub struct EdgeSchema {
    pub name: String,
    pub from: String,
    pub to: String,
    pub properties: Option<Vec<Field>>,
}

#[derive(Debug)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
}

#[derive(Debug, Clone)]
pub enum FieldType {
    String,
    Number,
    Boolean,
}

// Query-related structs
#[derive(Debug)]
pub struct Query {
    pub name: String,
    pub parameters: Vec<Parameter>,
    pub traversal: TraversalChain,
}

#[derive(Debug)]
pub struct Parameter {
    pub name: String,
}

#[derive(Debug)]
pub struct TraversalChain {
    pub start: StartStep,
    pub steps: Vec<Step>,
}

#[derive(Debug)]
pub enum StartStep {
    Vertex(Option<String>),
    Edge(Option<String>),
}

#[derive(Debug)]
pub enum Step {
    Colon(ColonStep),
    Filter(FilterStep),
}

#[derive(Debug)]
pub enum ColonStep {
    VertexStep(VertexStep),
    EdgeStep(EdgeStep),
    TypeStep(String),
}

#[derive(Debug)]
pub enum VertexStep {
    Out(Option<Vec<String>>),
    In(Option<Vec<String>>),
    Both(Option<Vec<String>>),
    OutV,
    InV,
    BothV,
}

#[derive(Debug)]
pub enum EdgeStep {
    OutE,
    InE,
    BothE,
}

#[derive(Debug)]
pub enum FilterStep {
    Has(Vec<Condition>),
    HasId(String),
}

#[derive(Debug)]
pub struct Condition {
    pub property: String,
    pub operator: ComparisonOp,
    pub value: Value,
}

#[derive(Debug)]
pub enum ComparisonOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    Neq,
}

#[derive(Debug)]
pub enum Value {
    String(String),
    Number(i64),
    Boolean(bool),
    Null,
    Identifier(String),
}

impl HelixParser {
    pub fn parse_source(input: &str) -> Result<Source, ParserError> {
        // assert!(false, "string: {:?}", input);
        let pairs = match HelixParser::parse(Rule::source, input) {
            Ok(mut pairs) => match pairs.next() {
                Some(pair) => pair,
                None => return Err(ParserError::from("No pairs found")),
            },
            Err(err) => return Err(ParserError::from(err)),
        };

        let mut source = Source {
            node_schemas: Vec::new(),
            edge_schemas: Vec::new(),
            queries: Vec::new(),
        };
        for pair in pairs.into_inner() {
            match pair.as_rule() {
                Rule::node_def => {
                    source.node_schemas.push(Self::parse_node_def(pair));
                }
                Rule::edge_def => {
                    source.edge_schemas.push(Self::parse_edge_def(pair));
                }
                Rule::query_def => {
                    source.queries.push(Self::parse_query_def(pair));
                }
                Rule::EOI => (),
                _ => {
                    // Print out unexpected rule for debugging
                    eprintln!("Unexpected rule: {:?}", pair.as_rule());
                    panic!("Unexpected rule encountered during parsing");
                }
            }
        }

        Ok(source)
    }

    fn parse_node_def(pair: Pair<Rule>) -> NodeSchema {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let fields = Self::parse_node_body(pairs.next().unwrap());
        NodeSchema { name, fields }
    }

    fn parse_node_body(pair: Pair<Rule>) -> Vec<Field> {
        let field_defs = pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::field_defs)
            .expect("Expected field_defs in properties");

        // Now parse each individual field_def
        field_defs
            .into_inner()
            .map(|p| Self::parse_field_def(p))
            .collect()
    }

    fn parse_edge_def(pair: Pair<Rule>) -> EdgeSchema {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let body = pairs.next().unwrap();
        let mut body_pairs = body.into_inner();

        let from = body_pairs.next().unwrap().as_str().to_string();
        let to = body_pairs.next().unwrap().as_str().to_string();
        let properties = body_pairs.next().map(|p| Self::parse_properties(p));

        EdgeSchema {
            name,
            from,
            to,
            properties,
        }
    }

    fn parse_field_def(pair: Pair<Rule>) -> Field {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let field_type = match pairs.next().unwrap().as_str() {
            "String" => FieldType::String,
            "Number" => FieldType::Number,
            "Boolean" => FieldType::Boolean,
            _ => unreachable!(),
        };

        Field { name, field_type }
    }

    fn parse_properties(pair: Pair<Rule>) -> Vec<Field> {
        // First get the field_defs rule
        let field_defs = pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::field_defs)
            .expect("Expected field_defs in properties");

        // Now parse each individual field_def
        field_defs
            .into_inner()
            .map(|p| Self::parse_field_def(p))
            .collect()
    }

    fn parse_query_def(pair: Pair<Rule>) -> Query {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();

        let mut parameters = Vec::new();
        let mut next = pairs.next().unwrap();

        if next.as_rule() == Rule::params {
            parameters = Self::parse_params(next);
            next = pairs.next().unwrap();
        }

        let traversal = Self::parse_get_stmt(next);

        Query {
            name,
            parameters,
            traversal,
        }
    }

    fn parse_params(pair: Pair<Rule>) -> Vec<Parameter> {
        pair.into_inner()
            .filter_map(|p| {
                if p.as_rule() == Rule::param_def {
                    Some(Parameter {
                        name: p.as_str().to_string(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    fn parse_get_stmt(pair: Pair<Rule>) -> TraversalChain {
        let traversal = pair.into_inner().next().unwrap();
        Self::parse_traversal(traversal)
    }

    fn parse_traversal(pair: Pair<Rule>) -> TraversalChain {
        let mut pairs = pair.into_inner();
        let start_pair = pairs.next().unwrap();
        let start = match start_pair.as_rule() {
            Rule::start_vertex => {
                let id = start_pair
                    .into_inner()
                    .next()
                    .map(|p| p.as_str().to_string());
                StartStep::Vertex(id)
            }
            Rule::start_edge => {
                let id = start_pair
                    .into_inner()
                    .next()
                    .map(|p| p.as_str().to_string());
                StartStep::Edge(id)
            }
            _ => unreachable!(),
        };

        let steps = pairs
            .filter_map(|p| {
                if p.as_rule() == Rule::step {
                    Some(Self::parse_step(p))
                } else {
                    None
                }
            })
            .collect();

        TraversalChain { start, steps }
    }

    fn parse_step(pair: Pair<Rule>) -> Step {
        let inner = pair.into_inner().next().unwrap();
        match inner.as_rule() {
            Rule::colon_step => Step::Colon(Self::parse_colon_step(inner)),
            Rule::filter_step => Step::Filter(Self::parse_filter_step(inner)),
            _ => unreachable!(),
        }
    }

    fn parse_colon_step(pair: Pair<Rule>) -> ColonStep {
        let inner = pair.into_inner().next().unwrap();
        match inner.as_rule() {
            Rule::vertex_step => ColonStep::VertexStep(Self::parse_vertex_step(inner)),
            Rule::edge_step => ColonStep::EdgeStep(Self::parse_edge_step(inner)),
            Rule::type_step => ColonStep::TypeStep(inner.as_str().to_string()),
            _ => unreachable!(),
        }
    }

    fn parse_vertex_step(pair: Pair<Rule>) -> VertexStep {
        let step_str = pair.as_str();
        let args = pair
            .into_inner()
            .next()
            .map(|p| p.into_inner().map(|arg| arg.as_str().to_string()).collect());

        match step_str {
            s if s.starts_with("Out(") || s == "Out" => VertexStep::Out(args),
            s if s.starts_with("In(") || s == "In" => VertexStep::In(args),
            s if s.starts_with("Both(") || s == "Both" => VertexStep::Both(args),
            "OutV" => VertexStep::OutV,
            "InV" => VertexStep::InV,
            "BothV" => VertexStep::BothV,
            _ => unreachable!(),
        }
    }

    fn parse_edge_step(pair: Pair<Rule>) -> EdgeStep {
        match pair.as_str() {
            "OutE" => EdgeStep::OutE,
            "InE" => EdgeStep::InE,
            "BothE" => EdgeStep::BothE,
            _ => unreachable!(),
        }
    }

    fn parse_filter_step(pair: Pair<Rule>) -> FilterStep {
        println!("Debug: Parsing filter step: '{}'", pair.as_str());
        let inner = pair.into_inner().next().unwrap();

        match inner.as_rule() {
            Rule::has => {
                let has_pairs: Vec<_> = inner.into_inner().collect();
                println!("Debug: Has pairs count: {}", has_pairs.len());

                let conditions = has_pairs
                    .into_iter()
                    .map(|p| {
                        println!("Debug: Processing has condition: '{}'", p.as_str());
                        Self::parse_condition(p)
                    })
                    .collect();

                FilterStep::Has(conditions)
            }
            Rule::has_id => {
                let id = inner.into_inner().next().unwrap().as_str().to_string();
                FilterStep::HasId(id)
            }
            _ => unreachable!("Unexpected filter step rule: {:?}", inner.as_rule()),
        }
    }

    fn parse_condition(pair: Pair<Rule>) -> Condition {
        let pairs: Vec<Pair<Rule>> = pair.into_inner().collect();
        let inner_pairs: Vec<Pair<Rule>> = pairs[0].clone().into_inner().collect();

        // Debug print all pairs
        for (i, p) in inner_pairs.iter().enumerate() {
            println!(
                "Debug: Pair {}: Rule={:?}, Text='{}'",
                i,
                p.as_rule(),
                p.as_str()
            );
        }

        if inner_pairs.len() < 3 {
            panic!(
                "Expected at least 3 parts in condition (property, operator, value), got {}",
                inner_pairs.len()
            );
        }

        let property = inner_pairs[0].as_str().to_string();

        let operator = match inner_pairs[1].as_str() {
            "=" => ComparisonOp::Eq,
            ">" => ComparisonOp::Gt,
            "<" => ComparisonOp::Lt,
            ">=" => ComparisonOp::Gte,
            "<=" => ComparisonOp::Lte,
            "!=" => ComparisonOp::Neq,
            other => panic!("Invalid operator: '{}'", other),
        };

        let value = match inner_pairs[2].as_rule() {
            Rule::identifier => Value::Identifier(inner_pairs[2].as_str().to_string()),
            Rule::string_literal => {
                let inner = inner_pairs[2]
                    .clone()
                    .into_inner()
                    .next()
                    .expect("String literal should have inner value");
                Value::String(inner.as_str().to_string())
            }
            Rule::number => {
                let num_str = inner_pairs[2].as_str();
                Value::Number(
                    num_str
                        .parse()
                        .unwrap_or_else(|_| panic!("Invalid number: {}", num_str)),
                )
            }
            Rule::boolean => Value::Boolean(inner_pairs[2].as_str() == "true"),
            Rule::null => Value::Null,
            rule => panic!("Unexpected value type: {:?}", rule),
        };

        Condition {
            property,
            operator,
            value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_node_schema() {
        let input = r#"
        N::User {
            Name: String,
            Age: Number
        }
        "#;

        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.node_schemas.len(), 1);
        assert_eq!(result.node_schemas[0].name, "User");
        assert_eq!(result.node_schemas[0].fields[0].name, "Name");
        assert_eq!(result.node_schemas[0].fields[1].name, "Age");
        assert_eq!(result.node_schemas[0].fields.len(), 2);
    }

    #[test]
    fn test_parse_edge_schema() {
        let input = r#"
        E::Follows {
            From: User,
            To: User,
            Properties {
                Since: String,
                Starting: Number
            }
        }
        "#;

        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.edge_schemas.len(), 1);
        assert_eq!(result.edge_schemas[0].name, "Follows");
        assert_eq!(result.edge_schemas[0].from, "User");
        assert_eq!(result.edge_schemas[0].to, "User");
        assert_eq!(
            result.edge_schemas[0].properties.as_ref().unwrap()[0].name,
            "Since"
        );
        assert_eq!(
            result.edge_schemas[0].properties.as_ref().unwrap()[1].name,
            "Starting"
        );
        assert_eq!(result.edge_schemas[0].properties.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_parse_query() {
        let input = r#"
        QUERY FindUser(userName) =>
            GET V::User HAS name = userName
        "#;

        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.queries.len(), 1);
        assert_eq!(result.queries[0].name, "FindUser");
        assert_eq!(result.queries[0].parameters.len(), 1);
    }
}
