use rusqlite::{Connection as SqliteConn, Result as SqliteResult, params, types::Value as RusqliteValue};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use crate::helix_engine::types::GraphError;

#[derive(Debug)]
pub enum IngestionError {
    SqliteError(rusqlite::Error),
    GraphError(GraphError),
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

// this is all stuff already there
// place holder for graphdb
//struct MyGraphDB {}
// graphdb implementation (create_node, create_edge, create_index)

#[derive(Debug)]
pub struct TableSchema {
    name: String,
    columns: Vec<ColumnInfo>,
    primary_keys: HashSet<String>,
    foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug)]
pub struct ForeignKey {
    from_table: String,
    from_column: String,
    to_table: String,
    to_column: String,
}

#[derive(Debug)]
pub struct ColumnInfo {
    name: String,
    data_type: String,
    is_primary_key: bool,
}

impl fmt::Display for ForeignKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{} → {}.{}",
            self.from_table,
            self.from_column,
            self.to_table,
            self.to_column
        )
    }
}

impl fmt::Display for ColumnInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pk_indicator = if self.is_primary_key { " (Primary Key)" } else { "" };
        write!(f, "{} ({}{})", self.name, self.data_type, pk_indicator)
    }
}

impl fmt::Display for TableSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // table header
        writeln!(f, "Table: {}", self.name)?;
        writeln!(f, "{}", "-".repeat(40))?;

        // columns section
        writeln!(f, "Columns:")?;
        if self.columns.is_empty() {
            writeln!(f, "  None")?;
        } else {
            for (i, column) in self.columns.iter().enumerate() {
                writeln!(f, "  {}. {}", i + 1, column)?;
            }
        }
        writeln!(f)?;

        // primary keys section
        writeln!(f, "Primary Keys:")?;
        if self.primary_keys.is_empty() {
            writeln!(f, "  None")?;
        } else {
            let mut pks: Vec<&String> = self.primary_keys.iter().collect();
            pks.sort(); // Sort for consistent output
            for pk in pks {
                writeln!(f, "  - {}", pk)?;
            }
        }
        writeln!(f)?;

        // foreign keys section
        writeln!(f, "Foreign Keys:")?;
        if self.foreign_keys.is_empty() {
            writeln!(f, "  None")?;
        } else {
            for (i, fk) in self.foreign_keys.iter().enumerate() {
                writeln!(f, "  {}. {}", i + 1, fk)?;
            }
        }
        writeln!(f, "{}", "-".repeat(40))?;

        Ok(())
    }
}

pub struct SqliteIngestor {
    pub sqlite_conn: SqliteConn,
    pub instance: String,
    pub batch_size: usize,
    pub id_mappings: HashMap<String, HashMap<String, NodeId>>,
}

impl SqliteIngestor {
    pub fn new(sqlite_path: &str, instance: Option<String>, batch_size: usize) -> Result<Self, IngestionError> {
        let sqlite_conn = SqliteConn::open(sqlite_path)?;

        Ok(SqliteIngestor {
            sqlite_conn,
            instance: instance.unwrap_or("http://localhost:6969".to_string()),
            batch_size,
            id_mappings: HashMap::new(),
        })
    }

    pub fn extract_schema(&mut self) -> Result<Vec<TableSchema>, IngestionError> {
        let mut schemas = Vec::new();

        // statement
        let mut stmt = self.sqlite_conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
        let table_names: Vec<String> = stmt.query_map(params![], |row| row.get(0))?
            .collect::<SqliteResult<Vec<String>>>()?;

        for table_name in table_names {
            let mut columns: Vec<ColumnInfo> = Vec::new();
            let mut primary_keys = HashSet::new();

            let mut col_stmt = self.sqlite_conn.prepare(&format!("PRAGMA table_info({})", table_name))?;
            let col_rows = col_stmt.query_map(params![], |row| {
                let name: String = row.get(1)?;
                let data_type: String = row.get(2)?;
                let is_pk: i32 = row.get(5)?;

                if is_pk > 0 {
                    primary_keys.insert(name.clone());
                }

                Ok(ColumnInfo {
                    name,
                    data_type,
                    is_primary_key: is_pk > 0,
                })
            })?;

            for col_res in col_rows {
                columns.push(col_res?);
            }

            let mut fk_stmt = self.sqlite_conn.prepare(&format!("PRAGMA foreign_key_list({})", table_name))?;
            let fk_rows = fk_stmt.query_map(params![], |row| {
                let to_table: String = row.get(2)?;
                let from_column: String = row.get(3)?;
                let to_column: String = row.get(4)?;

                Ok(ForeignKey {
                    from_table: table_name.clone(),
                    from_column,
                    to_table,
                    to_column,
                })
            })?;

            let mut foreign_keys: Vec<ForeignKey> = Vec::new();
            for fk_result in fk_rows {
                foreign_keys.push(fk_result?);
            }

            schemas.push(TableSchema {
                name: table_name,
                columns,
                primary_keys,
                foreign_keys,
            });
        }

        Ok(schemas)
    }

    pub fn ingest_table(&mut self, table_schema: &TableSchema) -> Result<(), IngestionError> {
        let count_query = format!("SELECT COUNT(*) FROM {}", table_schema.name);
        let max_rows: usize = self
            .sqlite_conn
            .query_row(&count_query, params![], |row| row.get(0))
            .map_err(|e| IngestionError::SqliteError(e))?;

        let query = format!("SELECT * FROM {}", table_schema.name);
        let mut stmt = self.sqlite_conn.prepare(&query)?;

        let column_names: Vec<String> = stmt.column_names().into_iter().map(String::from).collect();

        let mut table_id_mapping = HashMap::new();

        let mut row_count = 0;
        let mut rows = stmt.query(params![])?;

        // TODO: this the part that needs to be batched, idealy async as well
        while let Some(row) = rows.next()? {
            let mut properties = HashMap::new();
            let mut primary_key_value = String::new();

            for (i, col_name) in column_names.iter().enumerate() {
                let value: RusqliteValue = row.get(i).map_err(|e| {
                    IngestionError::MappingError(format!("Failed to get value for column {}: {}", col_name, e))
                })?;
                properties.insert(col_name.clone(), value.clone());

                // track primary key for creating edges
                if table_schema.primary_keys.contains(col_name) {
                    match value {
                        RusqliteValue::Text(s) => {
                            primary_key_value = s;
                        }
                        RusqliteValue::Integer(i) => {
                            primary_key_value = i.to_string();
                        }
                        _ => {
                            return Err(IngestionError::MappingError(format!(
                                        "Unsupported primary key type for column {}",
                                        col_name
                            )));
                        }
                    }
                }
            }

            row_count += 1;

            if row_count % self.batch_size == 0 || row_count == max_rows {
                // batch send data to helixdb server
                println!("TMP: SEND BATCH!");
            }
        }

        self.id_mappings.insert(table_schema.name.clone(), table_id_mapping);
        println!("Completed migrating {} rows from table {}", row_count, table_schema.name);

        Ok(())
    }

    // fn create_edges()
    // fn verify_ingestion
    // ...

    pub fn ingest(&mut self) -> Result<(), IngestionError> {
        let schemas = self.extract_schema()?;

        for schema in &schemas {
            self.ingest_table(schema)?;
        }

        // create edges
        // create indexes

        Ok(())
    }
}
