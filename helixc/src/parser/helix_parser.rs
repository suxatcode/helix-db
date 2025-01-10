use std::collections::HashMap;

use super::parser_methods::ParserError;
use pest::{iterators::Pair, Parser as PestParser};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct HelixParser;

// AST Structures
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

#[derive(Debug)]
pub struct Query {
    pub name: String,
    pub parameters: Vec<Parameter>,
    pub statements: Vec<Statement>,
    pub return_values: Vec<Expression>,
}

#[derive(Debug)]
pub struct Parameter {
    pub name: String,
}

#[derive(Debug)]
pub enum Statement {
    Assignment(Assignment),
    AddVertex(AddVertex),
    AddEdge(AddEdge),
}

#[derive(Debug)]
pub struct Assignment {
    pub variable: String,
    pub value: Expression,
}
#[derive(Debug)]
pub enum Expression {
    Traversal(Box<Traversal>),
    Identifier(String),
    StringLiteral(String),
    NumberLiteral(i32),
    BooleanLiteral(bool),
    Exists(Box<Traversal>),
    AddVertex(AddVertex),
    AddEdge(AddEdge),
}

#[derive(Debug)]
pub struct Traversal {
    pub start: StartNode,
    pub steps: Vec<Step>,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum Step {
    Vertex(GraphStep),
    Edge(GraphStep),
    Props(Vec<String>),
    Where(Box<Expression>),
    Exists(Box<Traversal>),
    BooleanOperation(BooleanOp),
    AddField(Vec<FieldAddition>),
    Count,
}

#[derive(Debug)]
pub struct FieldAddition {
    pub name: String,
    pub value: Expression,
}

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct AddVertex {
    pub vertex_type: Option<String>,
    pub fields: Option<Vec<(String, FieldValues)>>,
}

#[derive(Debug)]
pub struct AddEdge {
    pub edge_type: Option<String>,
    pub fields: Option<Vec<(String, FieldValues)>>,
    pub connection: EdgeConnection,
}

#[derive(Debug)]
pub struct EdgeConnection {
    from_id: String,
    to_id: String,
}

#[derive(Debug)]
pub enum FieldValues {
    String(String),
    Integer(i32),
    Float(f64),
    Boolean(bool),
}

impl From<String> for FieldValues {
    fn from(s: String) -> Self {
        FieldValues::String(s)
    }
}

impl From<i32> for FieldValues {
    fn from(i: i32) -> Self {
        FieldValues::Integer(i)
    }
}

impl From<f64> for FieldValues {
    fn from(f: f64) -> Self {
        FieldValues::Float(f)
    }
}

impl From<bool> for FieldValues {
    fn from(b: bool) -> Self {
        FieldValues::Boolean(b)
    }
}

impl HelixParser {
    pub fn parse_source(input: &str) -> Result<Source, ParserError> {
        let file = match HelixParser::parse(Rule::source, input) {
            Ok(mut pairs) => pairs
                .next()
                .ok_or_else(|| ParserError::from("Empty input"))?,
            Err(e) => return Err(ParserError::from(e)),
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
            "Number" => FieldType::Number,
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
            .map(|p| Parameter {
                name: p.as_str().to_string(),
            })
            .collect()
    }

    fn parse_query_body(pair: Pair<Rule>) -> Result<Vec<Statement>, ParserError> {
        pair.into_inner()
            .map(|p| match p.as_rule() {
                Rule::get_stmt => Ok(Statement::Assignment(Self::parse_get_statement(p)?)),
                Rule::AddV => Ok(Statement::AddVertex(Self::parse_add_vertex(p)?)),
                Rule::AddE => Ok(Statement::AddEdge(Self::parse_add_edge(p)?)),
                _ => Err(ParserError::from("Unexpected statement type in query body")),
            })
            .collect()
    }

    fn parse_add_vertex(pair: Pair<Rule>) -> Result<AddVertex, ParserError> {
        let mut vertex_type = None;
        let mut fields = None;

        println!("PAIR: {:?}", pair);
        for p in pair.into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    vertex_type = Some(p.as_str().to_string());
                }
                Rule::property_assignments => {
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
    ) -> Result<Vec<(String, FieldValues)>, ParserError> {
        Ok(pair
            .into_inner()
            .map(|p| {
                let mut pairs = p.into_inner();
                let prop_key = pairs.next().unwrap().as_str().to_string();
                let prop_val = pairs.next().unwrap().as_str().to_string();

                (prop_key, FieldValues::from(prop_val))
            })
            .collect::<Vec<(String, FieldValues)>>())
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
                Rule::property_assignments => {
                    fields = Some(Self::parse_property_assignments(p)?);
                }
                Rule::toFrom => {
                    connection = Some(Self::parse_to_from(p)?);
                }
                _ => return Err(ParserError::from(format!("Unexpected rule in AddE: {:?}", p.as_rule()))),
            }
        }

        Ok(AddEdge {
            edge_type,
            fields,
            connection: connection.ok_or_else(|| ParserError::from("Missing edge connection"))?,
        })
    }

    fn parse_id_args(pair: Pair<Rule>) -> Result<String, ParserError> {
        Ok(pair.into_inner().as_str().to_string())
    }

    fn parse_to_from(pair: Pair<Rule>) -> Result<EdgeConnection, ParserError> {
        let mut pairs = pair.into_inner();
        println!("TO FROM {:?}", pairs);
        let to_id = Self::parse_id_args(
            pairs
                .next()
                .ok_or_else(|| ParserError::from("Missing to IDs"))?,
        )?;
        let from_id = Self::parse_id_args(
            pairs
                .next()
                .ok_or_else(|| ParserError::from("Missing from IDs"))?,
        )?;

        Ok(EdgeConnection { to_id, from_id })
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

    fn parse_expression(p: Pair<Rule>) -> Result<Expression, ParserError> {
        let (l, c) = p.line_col();
        let pair = p.into_inner().next().unwrap();
        println!("l: {}, c: {}, Pair: {:?} {:?}", l, c, pair, pair.as_rule());
        match pair.as_rule() {
            Rule::traversal => Ok(Expression::Traversal(Box::new(Self::parse_traversal(
                pair,
            )?))),
            Rule::anonymous_traversal => Ok(Expression::Traversal(Box::new(
                Self::parse_traversal(pair)?,
            ))),
            Rule::identifier => Ok(Expression::Identifier(pair.as_str().to_string())),
            Rule::string_literal => Ok(Expression::StringLiteral(Self::parse_string_literal(pair))),
            Rule::number => Ok(Expression::NumberLiteral(pair.as_str().parse().unwrap())),
            Rule::boolean => Ok(Expression::BooleanLiteral(pair.as_str() == "true")),
            Rule::exists => Ok(Expression::Exists(Box::new(Self::parse_traversal(
                pair.into_inner().next().unwrap(),
            )?))),
            Rule::AddV => Ok(Expression::AddVertex(Self::parse_add_vertex(pair)?)),
            Rule::AddE => Ok(Expression::AddEdge(Self::parse_add_edge(pair)?)),
            _ => Err(ParserError::from("Unexpected expression type")),
        }
    }

    fn parse_string_literal(pair: Pair<Rule>) -> String {
        pair.into_inner().next().unwrap().as_str().to_string()
    }

    fn parse_traversal(pair: Pair<Rule>) -> Result<Traversal, ParserError> {
        println!(" HERE {:?}", pair.as_rule());
        let mut pairs = pair.into_inner();
        let start = Self::parse_start_node(pairs.next().unwrap())?;
        let steps = pairs
            .map(|p| Self::parse_step(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Traversal { start, steps })
    }

    fn parse_start_node(pair: Pair<Rule>) -> Result<StartNode, ParserError> {
        match pair.as_rule() {
            Rule::start_vertex => {
                let mut pairs = pair.into_inner();
                let types = pairs
                    .next()
                    .map(|p| p.into_inner().map(|t| t.as_str().to_string()).collect());
                let ids = pairs
                    .next()
                    .map(|p| p.into_inner().map(|id| id.as_str().to_string()).collect());
                Ok(StartNode::Vertex { types, ids })
            }
            Rule::start_edge => {
                let mut pairs = pair.into_inner();
                let types = pairs
                    .next()
                    .map(|p| p.into_inner().map(|t| t.as_str().to_string()).collect());
                let ids = pairs
                    .next()
                    .map(|p| p.into_inner().map(|id| id.as_str().to_string()).collect());
                Ok(StartNode::Edge { types, ids })
            }
            Rule::identifier => Ok(StartNode::Variable(pair.as_str().to_string())),
            _ => Ok(StartNode::Anonymous),
        }
    }

    fn parse_step(pair: Pair<Rule>) -> Result<Step, ParserError> {
        let inner = pair.into_inner().next().unwrap();
        println!("HELP {:?}", inner.as_rule());
        match inner.as_rule() {
            Rule::graph_step => Ok(Step::Vertex(Self::parse_graph_step(inner))),
            Rule::props_step => Ok(Step::Props(Self::parse_props_step(inner))),
            Rule::where_step => Ok(Step::Where(Box::new(Self::parse_expression(inner)?))),
            Rule::exists => {
                println!("AEHNVOAENVOAENVOUNEQ");
                Ok(Step::Exists(Box::new(Self::parse_traversal(
                    inner.into_inner().next().unwrap(),
                )?)))
            }
            Rule::bool_operations => Ok(Step::BooleanOperation(Self::parse_bool_operation(inner)?)),
            Rule::addfield => Ok(Step::AddField(Self::parse_field_additions(inner)?)),
            Rule::count => Ok(Step::Count),
            _ => Err(ParserError::from("Unexpected step type")),
        }
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
            _ => unreachable!(),
        }
    }

    fn parse_props_step(pair: Pair<Rule>) -> Vec<String> {
        pair.into_inner().map(|p| p.as_str().to_string()).collect()
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
            .map(|p| Self::parse_new_field(p))
            .collect()
    }

    fn parse_new_field(pair: Pair<Rule>) -> Result<FieldAddition, ParserError> {
        let mut pairs = pair.into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let value_pair = pairs.next().unwrap();

        let value = match value_pair.as_rule() {
            Rule::evaluates_to_anything => Self::parse_expression(value_pair)?,
            Rule::anonymous_traversal => {
                Expression::Traversal(Box::new(Self::parse_traversal(value_pair)?))
            }
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected field value type: {:?}",
                    value_pair.as_rule()
                )))
            }
        };

        Ok(FieldAddition { name, value })
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
            Age: Number
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
            Properties {
                Since: Number
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
        matches!(properties[0].field_type, FieldType::Number);
    }

    #[test]
    fn test_parse_edge_schema_no_props() {
        let input = r#"
        
        E::Follows {
            From: User,
            To: User,
            Properties {
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
        QUERY FindUser(userName) => 
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
        QUERY fetchUsers(name, age) =>
            user <- V<USER>(123)
            nameField <- user::Props(Name)
            ageField <- user::Props(Age)
            RETURN nameField, ageField
        "#;
        let result = HelixParser::parse_source(input).unwrap();
        assert_eq!(result.queries.len(), 1);
        let query = &result.queries[0];
        assert_eq!(query.name, "fetchUsers");
        assert_eq!(query.parameters.len(), 2);
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 2);
    }

    #[test]
    fn test_node_definition() {
        let input = r#"
        V::USER {
            ID: String,
            Name: String,
            Age: Number
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
            Properties {
                Since: String,
                Strength: Number
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
            Properties {
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
    QUERY logicalOps(id) =>
        user <- V<USER>(id)
        condition <- name::EQ("Alice")::Props(Age)
        RETURN condition
    "#;
        let result = HelixParser::parse_source(input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "logicalOps");
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_anonymous_traversal() {
        let input = r#"
    QUERY anonymousTraversal() =>
        result <- V::OutE<FRIENDSHIP>::InV::Props(Age)
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
        edge <- E<FRIENDSHIP>(45)
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
        QUERY userExists(id) =>
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
        user <- V<USER>(999)
        name <- user::Props(Name)
        age <- user::Props(Age)
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
        user <- V<USER>(123)
        enriched <- user::{Name: "name", Follows: _::Out<Follows>::Props(Age)}
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
        user <- V<USER>(789)
        friends <- user::Out<FRIENDSHIP>::InV::WHERE(_::Out)
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
        edge <- AddE<Rating>({Rating: 5})::To(123)::From(456)
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
        assert_eq!(query.statements.len(), 1);
    }
}
