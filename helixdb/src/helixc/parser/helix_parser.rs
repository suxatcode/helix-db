use super::parser_methods::ParserError;
use crate::protocol::value::Value;
use pest::{
    iterators::{Pair, Pairs},
    Parser as PestParser,
};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct HelixParser;

// AST Structures
#[derive(Debug, Clone)]
pub struct Source {
    pub node_schemas: Vec<NodeSchema>,
    pub edge_schemas: Vec<EdgeSchema>,
    pub queries: Vec<Query>,
}

#[derive(Debug, Clone)]
pub struct NodeSchema {
    pub name: String,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone)]
pub struct EdgeSchema {
    pub name: String,
    pub from: String,
    pub to: String,
    pub properties: Option<Vec<Field>>,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub name: String,
    pub parameters: Vec<Parameter>,
    pub statements: Vec<Statement>,
    pub return_values: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct Parameter {
    pub name: String,
    pub param_type: FieldType,
}

#[derive(Debug, Clone)]
pub enum Statement {
    Assignment(Assignment),
    AddVertex(AddVertex),
    AddEdge(AddEdge),
    Drop(Expression),
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub variable: String,
    pub value: Expression,
}

#[derive(Debug, Clone)]
pub enum Expression {
    Traversal(Box<Traversal>),
    Identifier(String),
    StringLiteral(String),
    IntegerLiteral(i32),
    FloatLiteral(f64),
    BooleanLiteral(bool),
    Exists(Box<Traversal>),
    AddVertex(AddVertex),
    AddEdge(AddEdge),
    And(Vec<Expression>),
    Or(Vec<Expression>),
    None,
}

#[derive(Debug, Clone)]
pub struct Traversal {
    pub start: StartNode,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone)]
pub enum StartNode {
    Vertex {
        types: Option<Vec<String>>,
        ids: Option<Vec<String>>,
    },
    Edge {
        types: Option<Vec<String>>,
        ids: Option<Vec<String>>,
    },
    Variable(String),
    Anonymous,
}

#[derive(Debug, Clone)]
pub enum Step {
    Vertex(GraphStep),
    Edge(GraphStep),
    Props(Vec<String>),
    Where(Box<Expression>),
    BooleanOperation(BooleanOp),
    Count,
    ID,
    Update(Update),
    Object(Object),
    Exclude(Exclude),
    Closure(Closure),
    Range((Expression, Expression)),
}

#[derive(Debug, Clone)]
pub struct FieldAddition {
    pub name: String,
    pub value: FieldValue,
}

#[derive(Debug, Clone)]
pub enum FieldValue {
    Traversal(Box<Traversal>),
    Expression(Expression),
    Fields(Vec<FieldAddition>),
    Literal(Value),
    Empty,
}

#[derive(Debug, Clone)]
pub enum GraphStep {
    Out(Option<Vec<String>>),
    In(Option<Vec<String>>),
    Both(Option<Vec<String>>),
    OutV,
    InV,
    BothV,
    OutE(Option<Vec<String>>),
    InE(Option<Vec<String>>),
    BothE(Option<Vec<String>>),
}

#[derive(Debug, Clone)]
pub enum BooleanOp {
    And(Vec<Expression>),
    Or(Vec<Expression>),
    GreaterThan(Box<Expression>),
    GreaterThanOrEqual(Box<Expression>),
    LessThan(Box<Expression>),
    LessThanOrEqual(Box<Expression>),
    Equal(Box<Expression>),
    NotEqual(Box<Expression>),
}

#[derive(Debug, Clone)]
pub struct AddVertex {
    pub vertex_type: Option<String>,
    pub fields: Option<Vec<(String, ValueType)>>,
}

#[derive(Debug, Clone)]
pub struct AddEdge {
    pub edge_type: Option<String>,
    pub fields: Option<Vec<(String, ValueType)>>,
    pub connection: EdgeConnection,
}

#[derive(Debug, Clone)]
pub struct EdgeConnection {
    pub from_id: IdType,
    pub to_id: IdType,
}

#[derive(Debug, Clone)]
pub enum IdType {
    Literal(String),
    Identifier(String),
}

#[derive(Debug, Clone)]
pub enum ValueType {
    Literal(Value),
    Identifier(String),
    Object(Object),
}
impl From<Value> for ValueType {
    fn from(value: Value) -> ValueType {
        match value {
            Value::String(s) => ValueType::Literal(Value::String(s)),
            Value::Integer(i) => ValueType::Literal(Value::Integer(i)),
            Value::Float(f) => ValueType::Literal(Value::Float(f)),
            Value::Boolean(b) => ValueType::Literal(Value::Boolean(b)),
            Value::Array(arr) => ValueType::Literal(Value::Array(arr)),
            Value::Empty => ValueType::Literal(Value::Empty),
            _ => unreachable!(),
        }
    }
}

impl From<IdType> for String {
    fn from(id_type: IdType) -> String {
        match id_type {
            IdType::Literal(mut s) => {
                s.retain(|c| c != '"');
                s
            }
            IdType::Identifier(s) => s,
        }
    }
}

impl From<String> for IdType {
    fn from(mut s: String) -> IdType {
        s.retain(|c| c != '"');
        IdType::Literal(s)
    }
}

#[derive(Debug, Clone)]
pub struct Update {
    pub fields: Vec<FieldAddition>,
}

#[derive(Debug, Clone)]
pub struct Object {
    pub fields: Vec<(String, FieldValue)>,
    pub should_spread: bool,
}

#[derive(Debug, Clone)]
pub struct Exclude {
    pub fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Closure {
    pub identifier: String,
    pub object: Object,
}


impl HelixParser {
    pub fn parse_source(input: &str) -> Result<Source, ParserError> {
        let file = match HelixParser::parse(Rule::source, input) {
            Ok(mut pairs) => {
                let pair = pairs
                    .next()
                    .ok_or_else(|| ParserError::from("Empty input"))?;
                pair
            }
            Err(e) => {
                return Err(ParserError::from(e));
            }
        };

        let mut source = Source {
            node_schemas: Vec::new(),
            edge_schemas: Vec::new(),
            queries: Vec::new(),
        };

        for pair in file.into_inner() {
            match pair.as_rule() {
                Rule::node_def => source.node_schemas.push(Self::parse_node_def(pair)),
                Rule::edge_def => source.edge_schemas.push(Self::parse_edge_def(pair)),
                Rule::query_def => source.queries.push(Self::parse_query_def(pair)?),
                Rule::EOI => (),
                _ => return Err(ParserError::from("Unexpected rule encountered")),
            }
        }

        Ok(source)
    }

    fn parse_field_defs(pair: Pair<Rule>) -> Vec<Field> {
        pair.into_inner()
            .map(|p| Self::parse_field_def(p))
            .collect()
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

    fn parse_field_def(pair: Pair<Rule>) -> Field {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let field_type = match pairs.next().unwrap().as_str() {
            "String" => FieldType::String,
            "Integer" => FieldType::Integer,
            "Float" => FieldType::Float,
            "Boolean" => FieldType::Boolean,
            _ => unreachable!(),
        };

        Field { name, field_type }
    }

    fn parse_edge_def(pair: Pair<Rule>) -> EdgeSchema {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let body = pairs.next().unwrap();
        let mut body_pairs = body.into_inner();

        let from = body_pairs.next().unwrap().as_str().to_string();
        let to = body_pairs.next().unwrap().as_str().to_string();
        let properties = Some(Self::parse_properties(body_pairs.next().unwrap()));

        EdgeSchema {
            name,
            from,
            to,
            properties,
        }
    }
    fn parse_properties(pair: Pair<Rule>) -> Vec<Field> {
        pair.into_inner()
            .find(|p| p.as_rule() == Rule::field_defs)
            .map_or(Vec::new(), |field_defs| {
                field_defs
                    .into_inner()
                    .map(|p| Self::parse_field_def(p))
                    .collect()
            })
    }

    fn parse_query_def(pair: Pair<Rule>) -> Result<Query, ParserError> {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let parameters = Self::parse_parameters(pairs.next().unwrap());
        let nect = pairs.next().unwrap();
        let statements = Self::parse_query_body(nect)?;
        let return_values = Self::parse_return_statement(pairs.next().unwrap())?;

        Ok(Query {
            name,
            parameters,
            statements,
            return_values,
        })
    }

    fn parse_parameters(pair: Pair<Rule>) -> Vec<Parameter> {
        pair.into_inner()
            .map(|p| {
                let mut inner = p.into_inner();
                let name = inner.next().unwrap().as_str().to_string();
                let param_type = match inner.next().unwrap().as_str() {
                    "String" => FieldType::String,
                    "Integer" => FieldType::Integer,
                    "Float" => FieldType::Float,
                    "Boolean" => FieldType::Boolean,
                    _ => unreachable!(),
                };
                Parameter { name, param_type }
                //hi
            })
            .collect()
    }

    fn parse_query_body(pair: Pair<Rule>) -> Result<Vec<Statement>, ParserError> {
        pair.into_inner()
            .map(|p| match p.as_rule() {
                Rule::get_stmt => Ok(Statement::Assignment(Self::parse_get_statement(p)?)),
                Rule::AddV => Ok(Statement::AddVertex(Self::parse_add_vertex(p)?)),
                Rule::AddE => Ok(Statement::AddEdge(Self::parse_add_edge(p)?)),
                Rule::drop => Ok(Statement::Drop(Self::parse_expression(p)?)),
                _ => Err(ParserError::from(format!(
                    "Unexpected statement type in query body: {:?}",
                    p.as_rule()
                ))),
            })
            .collect()
    }

    fn parse_add_vertex(pair: Pair<Rule>) -> Result<AddVertex, ParserError> {
        let mut vertex_type = None;
        let mut fields = None;

        for p in pair.into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    vertex_type = Some(p.as_str().to_string());
                }
                Rule::create_field => {
                    fields = Some(Self::parse_property_assignments(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddV: {:?} => {:?}",
                        p.as_rule(),
                        p,
                    )))
                }
            }
        }

        Ok(AddVertex {
            vertex_type,
            fields,
        })
    }

    fn parse_property_assignments(
        pair: Pair<Rule>,
    ) -> Result<Vec<(String, ValueType)>, ParserError> {
        pair.into_inner()
            .map(|p| {
                let mut pairs = p.into_inner();
                let prop_key = pairs
                    .next()
                    .ok_or_else(|| ParserError::from("Missing property key"))?
                    .as_str()
                    .to_string();

                let prop_val = match pairs.next() {
                    Some(p) => {
                        let value_pair = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| ParserError::from("Empty property value"))?;

                        match value_pair.as_rule() {
                            Rule::string_literal => Ok(ValueType::from(Value::from(
                                value_pair.as_str().to_string(),
                            ))),
                            Rule::integer => value_pair
                                .as_str()
                                .parse()
                                .map(|i| ValueType::from(Value::Integer(i)))
                                .map_err(|_| ParserError::from("Invalid integer value")),
                            Rule::float => value_pair
                                .as_str()
                                .parse()
                                .map(|f| ValueType::from(Value::Float(f)))
                                .map_err(|_| ParserError::from("Invalid float value")),
                            Rule::boolean => Ok(ValueType::from(Value::Boolean(
                                value_pair.as_str() == "true",
                            ))),
                            Rule::identifier => {
                                Ok(ValueType::Identifier(value_pair.as_str().to_string()))
                            }
                            _ => Err(ParserError::from("Invalid property value type")),
                        }?
                    }
                    None => ValueType::from(Value::Empty),
                };

                Ok((prop_key, prop_val))
            })
            .collect()
    }

    fn parse_add_edge(pair: Pair<Rule>) -> Result<AddEdge, ParserError> {
        let mut edge_type = None;
        let mut fields = None;
        let mut connection = None;

        for p in pair.into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    edge_type = Some(p.as_str().to_string());
                }
                Rule::create_field => {
                    fields = Some(Self::parse_property_assignments(p)?);
                }
                Rule::to_from => {
                    connection = Some(Self::parse_to_from(p)?);
                }
                Rule::from_to => {
                    connection = Some(Self::parse_from_to(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddE: {:?}",
                        p.as_rule()
                    )))
                }
            }
        }

        Ok(AddEdge {
            edge_type,
            fields,
            connection: connection.ok_or_else(|| ParserError::from("Missing edge connection"))?,
        })
    }

    fn parse_id_args(pair: Pair<Rule>) -> Result<IdType, ParserError> {
        let p = pair
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::from("Missing ID"))?;
        match p.as_rule() {
            Rule::identifier => Ok(IdType::Identifier(p.as_str().to_string())),
            Rule::string_literal | Rule::inner_string => Ok(IdType::from(p.as_str().to_string())),
            _ => unreachable!(),
        }
    }

    fn parse_to_from(pair: Pair<Rule>) -> Result<EdgeConnection, ParserError> {
        let mut pairs = pair.into_inner();
        let to_id = Self::parse_id_args(
            pairs
                .next()
                .ok_or_else(|| ParserError::from("Missing to IDs"))?
                .into_inner()
                .next()
                .ok_or_else(|| ParserError::from("Missing to IDs"))?,
        )?;
        let from_id = Self::parse_id_args(
            pairs
                .next()
                .ok_or_else(|| ParserError::from("Missing from IDs"))?
                .into_inner()
                .next()
                .ok_or_else(|| ParserError::from("Missing to IDs"))?,
        )?;

        Ok(EdgeConnection { to_id, from_id })
    }

    fn parse_from_to(pair: Pair<Rule>) -> Result<EdgeConnection, ParserError> {
        let mut pairs = pair.into_inner();
        let from_id = Self::parse_id_args(
            pairs
                .next()
                .ok_or_else(|| ParserError::from("Missing to IDs"))?,
        )?;
        let to_id = Self::parse_id_args(
            pairs
                .next()
                .ok_or_else(|| ParserError::from("Missing from IDs"))?,
        )?;

        Ok(EdgeConnection { from_id, to_id })
    }

    fn parse_get_statement(pair: Pair<Rule>) -> Result<Assignment, ParserError> {
        let mut pairs = pair.into_inner();
        let variable = pairs.next().unwrap().as_str().to_string();
        let value = Self::parse_expression(pairs.next().unwrap())?;

        Ok(Assignment { variable, value })
    }

    fn parse_return_statement(pair: Pair<Rule>) -> Result<Vec<Expression>, ParserError> {
        pair.into_inner()
            .map(|p| Self::parse_expression(p))
            .collect()
    }

    fn parse_expression_vec(pairs: Pairs<Rule>) -> Result<Vec<Expression>, ParserError> {
        let mut expressions = Vec::new();
        for p in pairs {
            match p.as_rule() {
                Rule::anonymous_traversal => {
                    expressions.push(Expression::Traversal(Box::new(Self::parse_anon_traversal(
                        p,
                    )?)));
                }
                Rule::traversal => {
                    expressions.push(Expression::Traversal(Box::new(Self::parse_traversal(p)?)));
                }
                Rule::id_traversal => {
                    expressions.push(Expression::Traversal(Box::new(Self::parse_traversal(p)?)));
                }
                Rule::evaluates_to_bool => {
                    expressions.push(Self::parse_boolean_expression(p)?);
                }
                _ => unreachable!(),
            }
        }
        Ok(expressions)
    }

    fn parse_boolean_expression(pair: Pair<Rule>) -> Result<Expression, ParserError> {
        let expression = pair.into_inner().next().unwrap();
        match expression.as_rule() {
            Rule::and => Ok(Expression::And(Self::parse_expression_vec(
                expression.into_inner(),
            )?)),
            Rule::or => Ok(Expression::Or(Self::parse_expression_vec(
                expression.into_inner(),
            )?)),
            Rule::boolean => Ok(Expression::BooleanLiteral(expression.as_str() == "true")),
            Rule::exists => Ok(Expression::Exists(Box::new(Self::parse_anon_traversal(
                expression.into_inner().next().unwrap(),
            )?))),
            _ => unreachable!(),
        }
    }

    fn parse_expression(p: Pair<Rule>) -> Result<Expression, ParserError> {
        let pair = p
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::from("Empty expression"))?;

        match pair.as_rule() {
            Rule::traversal => Ok(Expression::Traversal(Box::new(Self::parse_traversal(
                pair,
            )?))),
            Rule::id_traversal => Ok(Expression::Traversal(Box::new(Self::parse_traversal(
                pair,
            )?))),
            Rule::anonymous_traversal => Ok(Expression::Traversal(Box::new(
                Self::parse_anon_traversal(pair)?,
            ))),
            Rule::identifier => Ok(Expression::Identifier(pair.as_str().to_string())),
            Rule::string_literal => {
                Ok(Expression::StringLiteral(Self::parse_string_literal(pair)?))
            }
            Rule::exists => {
                let traversal = pair
                    .into_inner()
                    .next()
                    .ok_or_else(|| ParserError::from("Missing exists traversal"))?;
                Ok(Expression::Exists(Box::new(match traversal.as_rule() {
                    Rule::traversal => Self::parse_traversal(traversal)?,
                    Rule::id_traversal => Self::parse_traversal(traversal)?,
                    _ => unreachable!(),
                })))
            }
            Rule::integer => pair
                .as_str()
                .parse()
                .map(Expression::IntegerLiteral)
                .map_err(|_| ParserError::from("Invalid integer literal")),
            Rule::float => pair
                .as_str()
                .parse()
                .map(Expression::FloatLiteral)
                .map_err(|_| ParserError::from("Invalid float literal")),
            Rule::boolean => Ok(Expression::BooleanLiteral(pair.as_str() == "true")),
            Rule::evaluates_to_bool => Ok(Self::parse_boolean_expression(pair)?),
            Rule::AddV => Ok(Expression::AddVertex(Self::parse_add_vertex(pair)?)),
            Rule::AddE => Ok(Expression::AddEdge(Self::parse_add_edge(pair)?)),
            Rule::none => Ok(Expression::None),
            _ => Err(ParserError::from(format!(
                "Unexpected expression type: {:?}",
                pair.as_rule()
            ))),
        }
    }

    fn parse_string_literal(pair: Pair<Rule>) -> Result<String, ParserError> {
        let inner = pair
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::from("Empty string literal"))?;

        let mut literal = inner.as_str().to_string();
        literal.retain(|c| c != '"');
        Ok(literal)
    }

    fn parse_traversal(pair: Pair<Rule>) -> Result<Traversal, ParserError> {
        let mut pairs = pair.into_inner();
        let start = Self::parse_start_node(pairs.next().unwrap())?;
        let steps = pairs
            .map(|p| Self::parse_step(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Traversal { start, steps })
    }

    fn parse_anon_traversal(pair: Pair<Rule>) -> Result<Traversal, ParserError> {
        let pairs = pair.into_inner();
        let start = StartNode::Anonymous;
        let steps = pairs
            .map(|p| Self::parse_step(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Traversal { start, steps })
    }

    fn parse_start_node(pair: Pair<Rule>) -> Result<StartNode, ParserError> {
        match pair.as_rule() {
            Rule::start_vertex => {
                let pairs = pair.into_inner();
                let mut types = None;
                let mut ids = None;
                for p in pairs {
                    match p.as_rule() {
                        Rule::type_args => {
                            types = Some(
                                p.into_inner()
                                    .map(|t| t.as_str().to_string())
                                    .collect::<Vec<_>>(),
                            );
                        }
                        Rule::id_args => {
                            ids = Some(
                                p.into_inner()
                                    .map(|id| id.as_str().to_string())
                                    .collect::<Vec<_>>(),
                            );
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(StartNode::Vertex { types, ids })
            }
            Rule::start_edge => {
                let pairs = pair.into_inner();
                let mut types = None;
                let mut ids = None;
                for p in pairs {
                    match p.as_rule() {
                        Rule::type_args => {
                            types = Some(
                                p.into_inner()
                                    .map(|t| t.as_str().to_string())
                                    .collect::<Vec<_>>(),
                            );
                        }
                        Rule::id_args => {
                            ids = Some(
                                p.into_inner()
                                    .map(|id| id.as_str().to_string())
                                    .collect::<Vec<_>>(),
                            );
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(StartNode::Edge { types, ids })
            }
            Rule::identifier => Ok(StartNode::Variable(pair.as_str().to_string())),
            _ => Ok(StartNode::Anonymous),
        }
    }

    fn parse_step(pair: Pair<Rule>) -> Result<Step, ParserError> {
        let inner = pair.clone().into_inner().next().unwrap();
        match inner.as_rule() {
            Rule::graph_step => Ok(Step::Vertex(Self::parse_graph_step(inner))),
            Rule::object_step => Ok(Step::Object(Self::parse_object_step(inner)?)),
            Rule::closure_step => Ok(Step::Closure(Self::parse_closure(inner)?)),
            Rule::where_step => Ok(Step::Where(Box::new(Self::parse_expression(inner)?))),
            Rule::range_step => 

                Ok(Step::Range(Self::parse_range(pair)?)),
            
            Rule::bool_operations => Ok(Step::BooleanOperation(Self::parse_bool_operation(inner)?)),
            Rule::count => Ok(Step::Count),
            Rule::ID => Ok(Step::ID),
            Rule::update => Ok(Step::Update(Self::parse_update(inner)?)),
            Rule::exclude_field => Ok(Step::Exclude(Self::parse_exclude(inner)?)),
            _ => Err(ParserError::from("Unexpected step type")),
        }
    }

    fn parse_range(pair: Pair<Rule>) -> Result<(Expression, Expression), ParserError> {
        let mut inner = pair.into_inner().next().unwrap().into_inner();
        println!("inner: {:?}", inner);
        let start = match Self::parse_expression(inner.next().unwrap()) {
            Ok(val) => val,
            Err(e) => return Err(e),
        };
        let end = match Self::parse_expression(inner.next().unwrap()) {
            Ok(val) => val,
            Err(e) => return Err(e),
        };

        Ok((start, end))
    }

    fn parse_graph_step(pair: Pair<Rule>) -> GraphStep {
        let rule_str = pair.as_str();
        let types = pair
            .into_inner()
            .next()
            .map(|p| p.into_inner().map(|t| t.as_str().to_string()).collect());

        match rule_str {
            s if s.starts_with("OutE") => GraphStep::OutE(types),
            s if s.starts_with("InE") => GraphStep::InE(types),
            s if s.starts_with("BothE") => GraphStep::BothE(types),
            s if s.starts_with("OutV") => GraphStep::OutV,
            s if s.starts_with("InV") => GraphStep::InV,
            s if s.starts_with("BothV") => GraphStep::BothV,
            s if s.starts_with("Out") => GraphStep::Out(types),
            s if s.starts_with("In") => GraphStep::In(types),
            s if s.starts_with("Both") => GraphStep::Both(types),
            // s if s.starts_with("Range") => GraphStep::Range(),
            _ => unreachable!(),
        }
    }

    

    fn parse_bool_operation(pair: Pair<Rule>) -> Result<BooleanOp, ParserError> {
        let inner = pair.into_inner().next().unwrap();
        let expr = match inner.as_rule() {
            Rule::GT => BooleanOp::GreaterThan(Box::new(Self::parse_expression(
                inner.into_inner().next().unwrap(),
            )?)),
            Rule::GTE => BooleanOp::GreaterThanOrEqual(Box::new(Self::parse_expression(
                inner.into_inner().next().unwrap(),
            )?)),
            Rule::LT => BooleanOp::LessThan(Box::new(Self::parse_expression(
                inner.into_inner().next().unwrap(),
            )?)),
            Rule::LTE => BooleanOp::LessThanOrEqual(Box::new(Self::parse_expression(
                inner.into_inner().next().unwrap(),
            )?)),
            Rule::EQ => BooleanOp::Equal(Box::new(Self::parse_expression(
                inner.into_inner().next().unwrap(),
            )?)),
            Rule::NEQ => BooleanOp::NotEqual(Box::new(Self::parse_expression(
                inner.into_inner().next().unwrap(),
            )?)),
            _ => return Err(ParserError::from("Invalid boolean operation")),
        };
        Ok(expr)
    }

    fn parse_field_additions(pair: Pair<Rule>) -> Result<Vec<FieldAddition>, ParserError> {
        pair.into_inner()
            .map(|p| Self::parse_new_field_pair(p))
            .collect()
    }

    fn parse_new_field_pair(pair: Pair<Rule>) -> Result<FieldAddition, ParserError> {
        let print_pair = pair.clone();
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let value_pair = pairs.next().unwrap();

        let value: FieldValue = match value_pair.as_rule() {
            Rule::evaluates_to_anything => {
                FieldValue::Expression(Self::parse_expression(value_pair)?)
            }
            Rule::anonymous_traversal => {
                FieldValue::Traversal(Box::new(Self::parse_traversal(value_pair)?))
            }
            Rule::object_step => FieldValue::Fields(Self::parse_field_additions(value_pair)?),
            Rule::string_literal => {
                FieldValue::Literal(Value::String(Self::parse_string_literal(value_pair)?))
            }
            Rule::integer => FieldValue::Literal(Value::Integer(
                value_pair
                    .as_str()
                    .parse()
                    .map_err(|_| ParserError::from("Invalid integer literal"))?,
            )),
            Rule::float => FieldValue::Literal(Value::Float(
                value_pair
                    .as_str()
                    .parse()
                    .map_err(|_| ParserError::from("Invalid float literal"))?,
            )),
            Rule::boolean => FieldValue::Literal(Value::Boolean(value_pair.as_str() == "true")),
            Rule::none => FieldValue::Empty,
            Rule::mapping_field => FieldValue::Fields(Self::parse_field_additions(value_pair)?),
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected field pair type: {:?} \n {:?} \n\n {:?}",
                    value_pair.as_rule(),
                    value_pair,
                    print_pair
                )))
            }
        };

        Ok(FieldAddition { name, value })
    }

    fn parse_new_field_value(pair: Pair<Rule>) -> Result<FieldValue, ParserError> {
        let print_pair = pair.clone();
        let value_pair = pair.into_inner().next().unwrap();
        let value: FieldValue = match value_pair.as_rule() {
            Rule::evaluates_to_anything => {
                FieldValue::Expression(Self::parse_expression(value_pair)?)
            }
            Rule::anonymous_traversal => {
                FieldValue::Traversal(Box::new(Self::parse_traversal(value_pair)?))
            }
            Rule::object_step => FieldValue::Fields(Self::parse_field_additions(value_pair)?),
            Rule::string_literal => {
                FieldValue::Literal(Value::String(Self::parse_string_literal(value_pair)?))
            }
            Rule::integer => FieldValue::Literal(Value::Integer(
                value_pair
                    .as_str()
                    .parse()
                    .map_err(|_| ParserError::from("Invalid integer literal"))?,
            )),
            Rule::float => FieldValue::Literal(Value::Float(
                value_pair
                    .as_str()
                    .parse()
                    .map_err(|_| ParserError::from("Invalid float literal"))?,
            )),
            Rule::boolean => FieldValue::Literal(Value::Boolean(value_pair.as_str() == "true")),
            Rule::none => FieldValue::Empty,
            Rule::mapping_field => FieldValue::Fields(Self::parse_field_additions(value_pair)?),
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected field value type: {:?} \n {:?} \n\n {:?}",
                    value_pair.as_rule(),
                    value_pair,
                    print_pair
                )))
            }
        };

        Ok(value)
    }

    fn parse_update(pair: Pair<Rule>) -> Result<Update, ParserError> {
        let fields = Self::parse_field_additions(pair)?;
        Ok(Update { fields })
    }

    fn parse_object_step(pair: Pair<Rule>) -> Result<Object, ParserError> {
        let mut fields = Vec::new();
        let mut should_spread = false;
        for p in pair.into_inner() {
            if p.as_rule() == Rule::spread_object {
                should_spread = true;
                continue;
            }
            let mut pairs = p.into_inner();
            let prop_key = pairs.next().unwrap().as_str().to_string();
            let field_addition = match pairs.next() {
                Some(p) => match p.as_rule() {
                    Rule::evaluates_to_anything => {
                        FieldValue::Expression(Self::parse_expression(p)?)
                    }
                    Rule::anonymous_traversal => {
                        FieldValue::Traversal(Box::new(Self::parse_traversal(p)?))
                    }
                    Rule::mapping_field => FieldValue::Fields(Self::parse_field_additions(p)?),
                    Rule::object_step => FieldValue::Fields(
                        Self::parse_object_step(p)?
                            .fields
                            .iter()
                            .map(|(k, v)| FieldAddition {
                                name: k.clone(),
                                value: v.clone(),
                            })
                            .collect(),
                    ),

                    _ => Self::parse_new_field_value(p)?,
                },
                None if prop_key.len() > 0 => FieldValue::Literal(Value::String(prop_key.clone())),
                None => FieldValue::Empty,
            };
            fields.push((prop_key, field_addition));
        }
        Ok(Object {
            fields,
            should_spread,
        })
    }

    fn parse_closure(pair: Pair<Rule>) -> Result<Closure, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let identifier = pairs.next().unwrap().as_str().to_string();
        let object = Self::parse_object_step(pairs.next().unwrap())?;
        Ok(Closure { identifier, object })
    }

    fn parse_exclude(pair: Pair<Rule>) -> Result<Exclude, ParserError> {
        let mut fields = Vec::new();
        for p in pair.into_inner() {
            fields.push(p.as_str().to_string());
        }
        Ok(Exclude { fields })
    }
}

// Tests module
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_node_schema() {
        let input = r#"
        V::User {
            Name: String,
            Age: Integer
        }
        "#;

        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.node_schemas.len(), 1);
        let schema = &result.node_schemas[0];
        assert_eq!(schema.name, "User");
        assert_eq!(schema.fields.len(), 2);
    }

    #[test]
    fn test_parse_edge_schema() {
        let input = r#"
        
        E::Follows {
            From: User,
            To: User,
            Properties: {
                Since: Float
            }
        }
        "#;

        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.edge_schemas.len(), 1);
        let schema = &result.edge_schemas[0];
        assert_eq!(schema.name, "Follows");
        assert_eq!(schema.from, "User");
        assert_eq!(schema.to, "User");
        assert!(schema.properties.is_some());
        let properties = schema.properties.as_ref().unwrap();
        assert_eq!(properties.len(), 1);
        assert_eq!(properties[0].name, "Since");
        matches!(properties[0].field_type, FieldType::Float);
    }

    #[test]
    fn test_parse_edge_schema_no_props() {
        let input = r#"
        
        E::Follows {
            From: User,
            To: User,
            Properties: {
            }
        }
        "#;

        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.edge_schemas.len(), 1);
        let schema = &result.edge_schemas[0];
        assert_eq!(schema.name, "Follows");
        assert_eq!(schema.from, "User");
        assert_eq!(schema.to, "User");
        assert!(schema.properties.is_some());
        let properties = schema.properties.as_ref().unwrap();
        assert_eq!(properties.len(), 0);
    }

    #[test]
    fn test_parse_query() {
        let input = r#"
        QUERY FindUser(userName : String) => 
            user <- V<User>
            RETURN user
        "#;

        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.queries.len(), 1);
        let query = &result.queries[0];
        assert_eq!(query.name, "FindUser");
        assert_eq!(query.parameters.len(), 1);
        assert_eq!(query.parameters[0].name, "userName");
        assert_eq!(query.statements.len(), 1);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_query_with_parameters() {
        let input = r#"
        QUERY fetchUsers(name: String, age: Integer) =>
            user <- V<USER>("123")
            nameField <- user::{Name}
            ageField <- user::{Age}
            RETURN nameField, ageField
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.queries.len(), 1);
        let query = &result.queries[0];
        assert_eq!(query.name, "fetchUsers");
        assert_eq!(query.parameters.len(), 2);
        assert_eq!(query.parameters[0].name, "name");
        assert_eq!(query.parameters[0].param_type, FieldType::String);
        assert_eq!(query.parameters[1].name, "age");
        assert_eq!(query.parameters[1].param_type, FieldType::Integer);
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 2);
    }

    #[test]
    fn test_node_definition() {
        let input = r#"
        V::USER {
            ID: String,
            Name: String,
            Age: Integer
        }
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.node_schemas.len(), 1);
        let schema = &result.node_schemas[0];
        assert_eq!(schema.name, "USER");
        assert_eq!(schema.fields.len(), 3);
    }

    #[test]
    fn test_edge_with_properties() {
        let input = r#"
        E::FRIENDSHIP {
            From: USER,
            To: USER,
            Properties: {
                Since: String,
                Strength: Integer
            }
        }
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.edge_schemas.len(), 1);
        let schema = &result.edge_schemas[0];
        assert_eq!(schema.name, "FRIENDSHIP");
        assert_eq!(schema.from, "USER");
        assert_eq!(schema.to, "USER");
        let props = schema.properties.as_ref().unwrap();
        assert_eq!(props.len(), 2);
    }

    #[test]
    fn test_multiple_schemas() {
        let input = r#"
        V::USER {
            ID: String,
            Name: String,
            Email: String
        }
        V::POST {
            ID: String,
            Content: String
        }
        E::LIKES {
            From: USER,
            To: POST,
            Properties: {
                Timestamp: String
            }
        }
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.node_schemas.len(), 2);
        assert_eq!(result.edge_schemas.len(), 1);
    }

    /// THESE FAIL
    ///
    ///
    ///

    #[test]
    fn test_logical_operations() {
        let input = r#"
    QUERY logicalOps(id : String) =>
        user <- V<USER>(id)
        condition <- user::{name}::EQ("Alice")
        condition2 <- user::{age}::GT(20)
        RETURN condition
    "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "logicalOps");
        assert_eq!(query.statements.len(), 3);
    }

    #[test]
    fn test_anonymous_traversal() {
        let input = r#"
    QUERY anonymousTraversal() =>
        result <- V::OutE<FRIENDSHIP>::InV::{Age}
        RETURN result
    "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "anonymousTraversal");
        assert_eq!(query.statements.len(), 1);
    }

    #[test]
    fn test_edge_traversal() {
        let input = r#"
    QUERY getEdgeInfo() =>
        edge <- E<FRIENDSHIP>("999")
        fromUser <- edge::OutE
        toUser <- edge::OutV
        RETURN fromUser, toUser

    "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 2);
    }

    #[test]
    fn test_exists_query() {
        let input = r#"
        QUERY userExists(id : String) =>
            user <- V<User>(id)
            result <- EXISTS(user::OutE::InV<User>)
            RETURN result
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.queries.len(), 1);
        let query = &result.queries[0];
        assert_eq!(query.name, "userExists");
        assert_eq!(query.parameters.len(), 1);
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_multiple_return_values() {
        let input = r#"
    QUERY returnMultipleValues() =>
        user <- V<USER>("999")
        name <- user::{Name}
        age <- user::{Age}
        RETURN name, age
    "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 2);
    }

    #[test]
    fn test_add_fields() {
        let input = r#"
    QUERY enrichUserData() =>
        user <- V<USER>("123")
        enriched <- user::{Name: "name", Follows: _::Out<Follows>::{Age}}
        RETURN enriched
    "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_query_with_count() {
        let input = r#"
    QUERY analyzeNetwork() =>
        user <- V<USER>("999")
        friends <- user::Out<FRIENDSHIP>::InV::WHERE(_::Out::COUNT::GT(0))
        friendCount <- activeFriends::COUNT
        RETURN friendCount
    "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
    }

    #[test]
    fn test_add_vertex_query() {
        let input = r#"
    QUERY analyzeNetwork() =>
        user <- AddV<User>({Name: "Alice"})
        RETURN user
    "#;
        let result = match HelixParser::parse_source(input) {
            Ok(result) => result,
            Err(e) => {
                println!("{:?}", e);
                panic!();
            }
        };
        let query = &result.queries[0];
        println!("{:?}", query);
        assert_eq!(query.statements.len(), 1);
    }

    #[test]
    fn test_add_edge_query() {
        let input = r#"
    QUERY analyzeNetwork() =>
        edge <- AddE<Rating>({Rating: 5})::To("123")::From("456")
        edge <- AddE<Rating>({Rating: 5, Date: "2025-01-01"})::To("123")::From("456")
        RETURN edge
    "#;
        let result = match HelixParser::parse_source(input) {
            Ok(result) => result,
            Err(e) => {
                println!("{:?}", e);
                panic!();
            }
        };
        let query = &result.queries[0];
        println!("{:?}", query);
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_adding_with_identifiers() {
        let input = r#"
    QUERY addUsers() =>
        user1 <- AddV<User>({Name: "Alice", Age: 30})
        user2 <- AddV<User>({Name: "Bob", Age: 25})
        AddE<Follows>({Since: "1.0"})::From(user1)::To(user2)
        RETURN user1, user2
    "#;

        let result = match HelixParser::parse_source(input) {
            Ok(result) => result,
            Err(e) => {
                println!("{:?}", e);
                panic!();
            }
        };
        println!("{:?}", result);
        let query = &result.queries[0];
        println!("{:?}", query);
        assert_eq!(query.statements.len(), 3);
    }

    #[test]
    fn test_where_with_props() {
        let input = r#"
    QUERY getFollows() =>
        user <- V<User>::WHERE(_::{Age}::GT(2))
        user <- V<User>::WHERE(_::GT(2))
        RETURN user, follows
        "#;
        let result = match HelixParser::parse_source(input) {
            Ok(result) => result,
            Err(_e) => {
                panic!();
            }
        };
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_drop_operation() {
        let input = r#"
        QUERY deleteUser(id: String) =>
            user <- V<USER>(id)
            DROP user
            DROP user::OutE
            DROP V::OutE
            RETURN user
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "deleteUser");
        assert_eq!(query.parameters.len(), 1);
        assert_eq!(query.statements.len(), 4);
    }

    #[test]
    fn test_update_operation() {
        let input = r#"
        QUERY updateUser(id: String) =>
            user <- V<USER>(id)
            x <- user::UPDATE({Name: "NewName"})
            l <- user::UPDATE({Name: "NewName", Age: 30})
            RETURN user
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "updateUser");
        assert_eq!(query.parameters.len(), 1);
        assert_eq!(query.statements.len(), 3);
    }

    #[test]
    fn test_complex_traversal_combinations() {
        let input = r#"
        QUERY complexTraversal() =>
            result1 <- V<User>::OutE<Follows>::InV<User>::{name}
            result2 <- V::WHERE(AND(
                _::{age}::GT(20),
                OR(_::{name}::EQ("Alice"), _::{name}::EQ("Bob"))
            ))
            result3 <- V<User>::{
                friends: _::Out<Follows>::InV::{name},
                avgFriendAge: _::Out<Follows>::InV::{age}::GT(25)
            }
            RETURN result1, result2, result3
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "complexTraversal");
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 3);
    }

    #[test]
    fn test_nested_property_operations() {
        let input = r#"
        QUERY nestedProps() =>
            user <- V<User>("123")
            // Test nested property operations
            result <- user::{
                basic: {
                    name: _::{name},
                    age: _::{age}
                },
                social: {
                    friends: _::Out<Follows>::COUNT,
                    groups: _::Out<BelongsTo>::InV<Group>::{name}
                }
            }
            RETURN result
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_complex_edge_operations() {
        let input = r#"
        QUERY edgeOperations() =>
            edge1 <- AddE<Follows>({since: "2024-01-01", weight: 0.8})::From("user1")::To("user2")
            edge2 <- E<Follows>::WHERE(_::{weight}::GT(0.5))
            edge3 <- edge2::UPDATE({weight: 1.0, updated: "2024-03-01"})
            RETURN edge1, edge2, edge3
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 3);
    }

    #[test]
    fn test_mixed_type_operations() {
        let input = r#"
        QUERY mixedTypes() =>
            v1 <- AddV<User>({
                name: "Alice",
                age: 25,
                active: true,
                score: 4.5
            })
            result <- V<User>::WHERE(OR(
                _::{age}::GT(20),
                _::{score}::LT(5.0)
            ))
            RETURN v1, result
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 2);
    }

    #[test]
    fn test_error_cases() {
        // Test missing return statement
        let missing_return = r#"
        QUERY noReturn() =>
            result <- V<User>()
        "#;
        assert!(HelixParser::parse_source(missing_return).is_err());

        // Test invalid property access
        let invalid_props = r#"
        QUERY invalidProps() =>
            result <- V<User>::{}
            RETURN result
        "#;
        assert!(HelixParser::parse_source(invalid_props).is_err());
    }

    #[test]
    fn test_complex_schema_definitions() {
        let input = r#"
        V::ComplexUser {
            ID: String,
            Name: String,
            Age: Integer,
            Score: Float,
            Active: Boolean
        }
        E::ComplexRelation {
            From: ComplexUser,
            To: ComplexUser,
            Properties: {
                StartDate: String,
                EndDate: String,
                Weight: Float,
                Valid: Boolean,
                Count: Integer
            }
        }
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.node_schemas.len(), 1);
        assert_eq!(result.edge_schemas.len(), 1);

        let node = &result.node_schemas[0];
        assert_eq!(node.fields.len(), 5);

        let edge = &result.edge_schemas[0];
        let props = edge.properties.as_ref().unwrap();
        assert_eq!(props.len(), 5);
    }

    #[test]
    fn test_query_chaining() {
        let input = r#"
        QUERY chainedOperations() =>
            result <- V<User>("123")
                ::OutE<Follows>
                ::InV<User>
                ::{name}
                ::EQ("Alice")
            filtered <- V<User>::WHERE(
                _::Out<Follows>
                    ::InV<User>
                    ::{age}
                    ::GT(25)
            )
            updated <- filtered
                ::UPDATE({status: "active"})
            has_updated <- updated::{status}
                ::EQ("active")
            RETURN result, filtered, updated, has_updated
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 4);
        assert_eq!(query.return_values.len(), 4);
    }

    #[test]
    fn test_property_assignments() {
        let input = r#"
        QUERY testProperties(age: Integer) =>
            user <- AddV<User>({
                name: "Alice",
                age: age
            })
            RETURN user
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.parameters.len(), 1);
    }

    #[test]
    fn test_map_operation() {
        let input = r#"
        QUERY mapOperation() =>
            user <- V<User>("123")
            mapped <- user::{name: "name", age: "age"}
            RETURN mapped
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_map_in_return() {
        let input = r#"
        QUERY mapInReturn() =>
            user <- V<User>("123")
            RETURN user::{
                name, 
                age
            }
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 1);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_complex_object_operations() {
        let input = r#"
        QUERY complexObjects() =>
            user <- V<User>("123")
            result <- user::{
                basic: {
                    name,
                    age
                },
                friends: _::Out<Follows>::InV::{
                    name,
                    mutualFriends: _::Out<Follows>::COUNT
                }
            }
            RETURN result
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_exclude_fields() {
        let input = r#"
        QUERY excludeFields() =>
            user <- V<User>("123")
            filtered <- user::!{password, secretKey}
            RETURN filtered
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_spread_operator() {
        let input = r#"
        QUERY spreadFields() =>
            user <- V<User>("123")
            result <- user::{
                newField: "value",
                ..
            }
            RETURN result
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_complex_update_operations() {
        let input = r#"
        QUERY updateUser() =>
            user <- V<User>("123")
            updated <- user::UPDATE({
                name: "New Name",
                age: 30,
                lastUpdated: "2024-03-01",
                friendCount: _::Out<Follows>::COUNT
            })
            RETURN updated
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_nested_traversals() {
        let input = r#"
        QUERY nestedTraversals() =>
            start <- V<User>("123")
            result <- start::Out<Follows>::InV<User>::Out<Likes>::InV<Post>::{title}
            filtered <- result::WHERE(_::{likes}::GT(10))
            RETURN filtered
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_combined_operations() {
        let input = r#"
        QUERY combinedOps() =>
            // Test combination of different operations
            user <- V<User>("123")
            friends <- user::Out<Follows>::InV<User>
            active <- friends::WHERE(_::{active}::EQ(true))
            result <- active::{
                name,
                posts: _::Out<Created>::InV<Post>::!{deleted}::{
                    title: title,
                    likes: _::In<Likes>::COUNT
                }
            }
            RETURN result
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 4);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_closure() {
        let input = r#"
        QUERY multipleLayers() =>
            result <- V<User>::|user|{
                posts: _::Out<Created>::{
                    user_id: user::ID
                }
            }
            RETURN result
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        println!("\n\nresult: {:?}\n\n", result);
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 1);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_complex_return_traversal() {
        let input = r#"
        QUERY returnTraversal() =>
            RETURN V<User>::|user|{
                posts: _::Out<Created>::{
                    user_id: user::ID
                }
            }::!{createdAt, lastUpdated}::{username: name, ..}
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.return_values.len(), 1);
    }
}
