use crate::helix_engine::ingestion_engine::sqlite::{SqliteIngestor, ColumnInfo};
use rusqlite::{Connection, Result as SqliteResult, params};
use std::collections::HashMap;

pub fn create_mock_sqlite_db(file_path: Option<&str>) -> SqliteResult<Connection> {
    let conn = match file_path {
        Some(path) => Connection::open(path)?,
        None => Connection::open_in_memory()?,
    };

    // Enable foreign key constraints
    conn.execute("PRAGMA foreign_keys = ON;", params![])?;

    conn.execute(
        r#"
        CREATE TABLE parents (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            grew_up_in TEXT NOT NULL
        )
        "#,
        params![],
    )?;

    conn.execute(
        r#"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            city TEXT NOT NULL,
            parent_id INTEGER,
            FOREIGN KEY (parent_id) REFERENCES parents(id)
        )
        "#,
        params![],
    )?;

    let parents_data = vec![
        (1, "Ryan Williams", 61, "Dallas"),
        (2, "David Christian", 47, "New York"),
        (3, "Lawrence Dorsey", 66, "Dallas"),
        (4, "Kayla Mendoza", 63, "New York"),
        (5, "Aaron Stewart", 60, "San Antonio"),
        (6, "Victoria Edwards", 50, "Philadelphia"),
        (7, "James Perry", 50, "San Antonio"),
        (8, "Taylor Riddle", 55, "San Diego"),
        (9, "Christopher Garcia", 46, "San Diego"),
        (10, "Julie Dudley", 44, "Los Angeles"),
        (11, "Linda Chen", 48, "Dallas"),
        (12, "Lisa Lee", 45, "Philadelphia"),
        (13, "Samantha Lewis", 60, "Phoenix"),
        (14, "Michael Silva", 42, "Los Angeles"),
        (15, "Sherry Pena", 42, "Chicago"),
        (16, "Joel Bolton", 46, "New York"),
        (17, "Samuel Jones", 40, "Phoenix"),
        (18, "Scott Jones", 49, "Phoenix"),
        (19, "Kevin Wright", 43, "San Diego"),
        (20, "Lisa Garza", 62, "Philadelphia"),
        ];

    for parent in parents_data {
        conn.execute(
            "INSERT INTO parents (id, name, age, grew_up_in) VALUES (?1, ?2, ?3, ?4)",
            params![parent.0, parent.1, parent.2, parent.3],
        )?;
    }

    let users_data = vec![
        (1, "Heather Pittman", 28, "Dallas", 8),
        (2, "Angela Wallace", 19, "San Diego", 19),
        (3, "Barry Kelly", 19, "Chicago", 5),
        (4, "Lisa Barnes", 33, "New York", 5),
        (5, "Stephen Reynolds", 33, "Dallas", 18),
        (6, "Seth Gomez", 27, "San Diego", 5),
        (7, "Michelle Vance", 21, "Houston", 11),
        (8, "Regina Kirby", 27, "Dallas", 20),
        (9, "Linda Johnson", 36, "Philadelphia", 10),
        (10, "Virginia Copeland", 39, "San Jose", 9),
        (11, "Timothy Reed", 30, "Los Angeles", 7),
        (12, "Ashley Olsen", 28, "Chicago", 4),
        (13, "Kelly Walter", 21, "Chicago", 8),
        (14, "Anita Manning", 25, "Philadelphia", 15),
        (15, "Carl Dillon", 26, "San Diego", 11),
        (16, "James Keller", 36, "Houston", 11),
        (17, "Joe Moore", 30, "San Diego", 9),
        (18, "Brian Silva", 30, "Phoenix", 11),
        (19, "Stephen Riley", 38, "Phoenix", 14),
        (20, "Carolyn Gonzalez", 19, "Los Angeles", 13),
        ];

    for user in users_data {
        conn.execute(
            "INSERT INTO users (id, name, age, city, parent_id) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![user.0, user.1, user.2, user.3, user.4],
        )?;
    }

    Ok(conn)
}

#[test]
fn test_ingest_basics() {
    let conn = create_mock_sqlite_db(None).expect("Failed to create mock database");

    let mut ingestor = SqliteIngestor {
        sqlite_conn: conn,
        instance: "http://localhost:6969".to_string(),
        batch_size: 10,
        id_mappings: HashMap::new(),
    };

    let schemas = ingestor.extract_schema().unwrap();
    println!("{:?}", schemas);
}
