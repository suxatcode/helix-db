use rusqlite::{Connection as SqliteConn, Result as SqliteResult, NO_PARAMS};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use crate::helix_engine::types::GraphError;

#[derive(Debug)]
enum IngestionError {
    SqliteError(rusqlite::Error),
    GraphError(GraphError), // TODO: this is already built in?
    MappingError(String),
}

impl fmt::Display for IngestionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IngestionError::SqliteError(e) => write!(f, "{}", e),
            IngestionError::GraphError(e) => write!(f, "{}", e),
            IngestionError::MappingError(e) => write!(f, "{}", e),
        }
    }
}

impl Error for IngestionError {}

impl From<rusqlite::Error> for IngestionError {
    fn from(error: rusqlite::Error) -> Self {
        IngestionError::SqliteError(error)
    }
}

// place holder for types in graph
type NodeId = u64;
type EdgeId = u64;

#[derive(Debug, Clone)]
enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Boolean(bool),
}

impl From<rusqlite::types::Value> for Value {
    fn from(value: rusqlite::types::Value) -> Self {
        match value {
            rusqlite::types::Value::Null => Value::Null,
            rusqlite::types::Value::Integer(i) => Value::Integer(i),
            rusqlite::types::Value::Real(f) => Value::Real(f),
            rusqlite::types::Value::Text(s) => Value::Text(s),
            rusqlite::types::Value::Blob(b) => Value::Blob(b),
        }
    }
}

// this is all stuff already there
// place holder for graphdb
//struct MyGraphDB {}
// graphdb implementation (create_node, create_edge, create_index)


struct ForeignKey {
    from_table: String,
    from_column: String,
    to_table: String,
    to_column: String,
}

struct TableSchema {
    name: String,
    columns: Vec<ColumnInfo>,
    primary_keys: HashSet<String>,
    foreign_keys: Vec<ForeignKey>,
}

struct ColumnInfo {
    name: String,
    data_type: String,
    is_primary_key: bool,
}

struct SqliteIngestor {
    sqlite_conn: SqliteConn,
    //helix graph (heed probably like in vector_core)
    batch_size: usize,
    id_mappings: HashMap<String, HashMap<String, NodeId>>,
}

impl SqliteIngestor {
    fn new(sqlite_path: &str, /*graph_db*/ batch_size: usize) -> Result<Self, IngestionError> {
        let sqlite_conn = SqliteConn::open(sqlite_path)?;

        Ok(SqliteIngestor {
            sqlite_conn,
            batch_size,
            id_mappings: HashMap::new(),
        })
    }


    // fn extract_schema
    // fn migrate_table
    // ...
    // fn ingest
}
