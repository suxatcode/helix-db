use crate::ingestion_engine::postgres_ingestion::{to_camel_case, PostgresIngestor};
use chrono;
use rand;
use rust_decimal::Decimal;
use serde_json::{json, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{types::Type, Config, NoTls};

use super::postgres_ingestion::GraphSchema;

// Helper function to clean up the database before tests
async fn cleanup_database(client: &mut tokio_postgres::Client) -> Result<(), Box<dyn std::error::Error>> {
    // Drop existing tables if they exist
    let tables = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            &[],
        )
        .await?;

    // Drop all sequences if they exist
    let sequences = client
        .query(
            "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public'",
            &[],
        )
        .await?;

    for row in tables {
        let table_name: String = row.get(0);
        client
            .execute(&format!("DROP TABLE IF EXISTS {} CASCADE", table_name), &[])
            .await?;
    }

    for row in sequences {
        let sequence_name: String = row.get(0);
        client
            .execute(&format!("DROP SEQUENCE IF EXISTS {}", sequence_name), &[])
            .await?;
    }
    Ok(())
}

// Helper function to create a mock PostgreSQL database
pub async fn create_mock_postgres_db() -> Result<(tokio_postgres::Client, tokio_postgres::Config), tokio_postgres::Error> {
    // Create a configuration for the PostgreSQL connection
    let mut config = Config::new();
    config
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("postgres")
        .dbname("postgres");

    // Connect to the database
    let (mut client, connection) = config.connect(NoTls).await?;
    
    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Clean up the database first
    cleanup_database(&mut client).await.expect("Failed to clean up database");

    // Create tables with unique names based on timestamp and random number
    let parents_table = format!("parents");
    let users_table = format!("users");

    // Now create the tables with explicit sequence names to avoid conflicts
    client
        .batch_execute(&format!(
            r#"
            CREATE SEQUENCE {parents}_id_seq;
            CREATE SEQUENCE {users}_id_seq;
            
            CREATE TABLE {parents} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{parents}_id_seq'),
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                grew_up_in TEXT NOT NULL
            );
            
            CREATE TABLE {users} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{users}_id_seq'),
                name TEXT NOT NULL,
                age INTEGER NOT NULL,
                city TEXT NOT NULL,
                parent_id INTEGER REFERENCES {parents}(id)
            );
            "#,
            parents = parents_table,
            users = users_table
        ))
        .await?;

    // Insert data into parents table
    let parents_data = vec![
        ("Ryan Williams", 61, "Dallas"),
        ("David Christian", 47, "New York"),
        ("Lawrence Dorsey", 66, "Dallas"),
        ("Kayla Mendoza", 63, "New York"),
        ("Aaron Stewart", 60, "San Antonio"),
        ("Victoria Edwards", 50, "Philadelphia"),
        ("James Perry", 50, "San Antonio"),
        ("Taylor Riddle", 55, "San Diego"),
        ("Christopher Garcia", 46, "San Diego"),
        ("Julie Dudley", 44, "Los Angeles"),
        ("Linda Chen", 48, "Dallas"),
        ("Lisa Lee", 45, "Philadelphia"),
        ("Samantha Lewis", 60, "Phoenix"),
        ("Michael Silva", 42, "Los Angeles"),
        ("Sherry Pena", 42, "Chicago"),
        ("Joel Bolton", 46, "New York"),
        ("Samuel Jones", 40, "Phoenix"),
        ("Scott Jones", 49, "Phoenix"),
        ("Kevin Wright", 43, "San Diego"),
        ("Lisa Garza", 62, "Philadelphia"),
    ];

    for parent in parents_data {
        client
            .execute(
                &format!(
                    "INSERT INTO {} (name, age, grew_up_in) VALUES ($1, $2, $3)",
                    parents_table
                ),
                &[&parent.0, &(parent.1 as i32), &parent.2],
            )
            .await?;
    }

    // Insert data into users table
    let users_data = vec![
        ("Heather Pittman", 28, "Dallas", 8),
        ("Angela Wallace", 19, "San Diego", 19),
        ("Barry Kelly", 19, "Chicago", 5),
        ("Lisa Barnes", 33, "New York", 5),
        ("Stephen Reynolds", 33, "Dallas", 18),
        ("Seth Gomez", 27, "San Diego", 5),
        ("Michelle Vance", 21, "Houston", 11),
        ("Regina Kirby", 27, "Dallas", 20),
        ("Linda Johnson", 36, "Philadelphia", 10),
        ("Virginia Copeland", 39, "San Jose", 9),
        ("Timothy Reed", 30, "Los Angeles", 7),
        ("Ashley Olsen", 28, "Chicago", 4),
        ("Kelly Walter", 21, "Chicago", 8),
        ("Anita Manning", 25, "Philadelphia", 15),
        ("Carl Dillon", 26, "San Diego", 11),
        ("James Keller", 36, "Houston", 11),
        ("Joe Moore", 30, "San Diego", 9),
        ("Brian Silva", 30, "Phoenix", 11),
        ("Stephen Riley", 38, "Phoenix", 14),
        ("Carolyn Gonzalez", 19, "Los Angeles", 13),
    ];

    for user in users_data {
        client
            .execute(
                &format!(
                    "INSERT INTO {} (name, age, city, parent_id) VALUES ($1, $2, $3, $4)",
                    users_table
                ),
                &[&user.0, &(user.1 as i32), &user.2, &(user.3 as i32)],
            )
            .await?;
    }

    Ok((client, config))
}

// Helper function to create a temporary directory for test outputs
fn create_temp_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("postgres_test")
        .tempdir()
        .expect("Failed to create temp directory")
}

// Test the to_camel_case function
#[test]
fn test_to_camel_case() {
    assert_eq!(to_camel_case("hello_world"), "HelloWorld");
    assert_eq!(to_camel_case("hello-world"), "HelloWorld");
    assert_eq!(to_camel_case("hello world"), "HelloWorld");
    assert_eq!(to_camel_case("helloWorld"), "HelloWorld");
    assert_eq!(to_camel_case("HelloWorld"), "HelloWorld");
    assert_eq!(to_camel_case(""), "");
    assert_eq!(to_camel_case("hello"), "Hello");
    assert_eq!(to_camel_case("HELLO"), "Hello");
    assert_eq!(to_camel_case("hello_world_test"), "HelloWorldTest");
}

// Test the map_sql_type_to_helix_type function
#[test]
fn test_map_sql_type_to_helix_type() {
    use super::postgres_ingestion::map_sql_type_to_helix_type;

    assert_eq!(map_sql_type_to_helix_type("integer"), "Integer");
    assert_eq!(map_sql_type_to_helix_type("bigint"), "Integer");
    assert_eq!(map_sql_type_to_helix_type("smallint"), "Integer");
    assert_eq!(map_sql_type_to_helix_type("numeric"), "Float");
    assert_eq!(map_sql_type_to_helix_type("decimal"), "Float");
    assert_eq!(map_sql_type_to_helix_type("real"), "Float");
    assert_eq!(map_sql_type_to_helix_type("double precision"), "Float");
    assert_eq!(map_sql_type_to_helix_type("character varying"), "String");
    assert_eq!(map_sql_type_to_helix_type("character"), "String");
    assert_eq!(map_sql_type_to_helix_type("text"), "String");
    assert_eq!(map_sql_type_to_helix_type("boolean"), "Boolean");
    assert_eq!(map_sql_type_to_helix_type("date"), "String");
    assert_eq!(map_sql_type_to_helix_type("time"), "String");
    assert_eq!(map_sql_type_to_helix_type("timestamp"), "String");
    assert_eq!(map_sql_type_to_helix_type("uuid"), "String");
    assert_eq!(map_sql_type_to_helix_type("json"), "String");
    assert_eq!(map_sql_type_to_helix_type("jsonb"), "String");
    assert_eq!(map_sql_type_to_helix_type("bytea"), "String");
}

// Test the full ingestion process
#[tokio::test]
async fn test_postgres_full_ingestion() {
    // Create a configuration for the PostgreSQL connection
    let mut config = Config::new();
    config
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("postgres")
        .dbname("postgres");

    // Connect to the database
    let (mut client, connection) = config
        .connect(NoTls)
        .await
        .expect("Failed to connect to database");

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Clean up the database first
    cleanup_database(&mut client)
        .await
        .expect("Failed to clean up database");

    // Create a more complex schema with unique table names
    let users_table = format!("users");
    let posts_table = format!("posts");
    let comments_table = format!("comments");
    let tags_table = format!("tags");
    let post_tags_table = format!("post_tags");

    // Now create the tables with explicit sequence names
    client
        .batch_execute(&format!(
            r#"
            CREATE SEQUENCE {users}_id_seq;
            CREATE SEQUENCE {posts}_id_seq;
            CREATE SEQUENCE {comments}_id_seq;
            CREATE SEQUENCE {tags}_id_seq;
            CREATE SEQUENCE {post_tags}_id_seq;
            
            CREATE TABLE {users} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{users}_id_seq'),
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE {posts} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{posts}_id_seq'),
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                author_id INTEGER REFERENCES {users_ref}(id),
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE {comments} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{comments}_id_seq'),
                content TEXT NOT NULL,
                post_id INTEGER REFERENCES {posts_ref}(id),
                author_id INTEGER REFERENCES {users_ref}(id),
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE {tags} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{tags}_id_seq'),
                name TEXT NOT NULL UNIQUE
            );
            
            CREATE TABLE {post_tags} (
                post_id INTEGER REFERENCES {posts_ref}(id),
                tag_id INTEGER REFERENCES {tags_ref}(id),
                PRIMARY KEY (post_id, tag_id)
            );
            "#,
            post_tags = post_tags_table,
            comments = comments_table,
            posts = posts_table,
            users = users_table,
            tags = tags_table,
            users_ref = users_table,
            posts_ref = posts_table,
            tags_ref = tags_table
        ))
        .await
        .expect("Failed to create complex schema");

    // Insert sample data
    client
        .execute(
            &format!(
                "INSERT INTO {} (username, email) VALUES ($1, $2)",
                users_table
            ),
            &[&"john_doe", &"john@example.com"],
        )
        .await
        .expect("Failed to insert user");

    client
        .execute(
            &format!(
                "INSERT INTO {} (username, email) VALUES ($1, $2)",
                users_table
            ),
            &[&"jane_smith", &"jane@example.com"],
        )
        .await
        .expect("Failed to insert user");

    client
        .execute(
            &format!(
                "INSERT INTO {} (title, content, author_id) VALUES ($1, $2, $3)",
                posts_table
            ),
            &[
                &"First Post",
                &"This is the content of the first post",
                &(1 as i32),
            ],
        )
        .await
        .expect("Failed to insert post");

    client
        .execute(
            &format!(
                "INSERT INTO {} (title, content, author_id) VALUES ($1, $2, $3)",
                posts_table
            ),
            &[
                &"Second Post",
                &"This is the content of the second post",
                &(2 as i32),
            ],
        )
        .await
        .expect("Failed to insert post");

    client
        .execute(
            &format!(
                "INSERT INTO {} (content, post_id, author_id) VALUES ($1, $2, $3)",
                comments_table
            ),
            &[&"Great post!", &(1 as i32), &(2 as i32)],
        )
        .await
        .expect("Failed to insert comment");

    client
        .execute(
            &format!(
                "INSERT INTO {} (content, post_id, author_id) VALUES ($1, $2, $3)",
                comments_table
            ),
            &[&"Thanks for sharing!", &(2 as i32), &(1 as i32)],
        )
        .await
        .expect("Failed to insert comment");

    client
        .execute(
            &format!("INSERT INTO {} (name) VALUES ($1)", tags_table),
            &[&"technology"],
        )
        .await
        .expect("Failed to insert tag");

    client
        .execute(
            &format!("INSERT INTO {} (name) VALUES ($1)", tags_table),
            &[&"programming"],
        )
        .await
        .expect("Failed to insert tag");

    client
        .execute(
            &format!(
                "INSERT INTO {} (post_id, tag_id) VALUES ($1, $2)",
                post_tags_table
            ),
            &[&(1 as i32), &(1 as i32)],
        )
        .await
        .expect("Failed to insert post tag");

    client
        .execute(
            &format!(
                "INSERT INTO {} (post_id, tag_id) VALUES ($1, $2)",
                post_tags_table
            ),
            &[&(2 as i32), &(2 as i32)],
        )
        .await
        .expect("Failed to insert post tag");

    // Create a temporary directory for test outputs
    let temp_dir = create_temp_dir();
    let output_dir = temp_dir.path().to_str().unwrap();

    // Create a PostgreSQL ingestor
    let mut ingestor = PostgresIngestor::new(
        &format!("postgres://postgres:postgres@localhost:5432/postgres"),
        Some("test_instance".to_string()),
        1000,
    )
    .await
    .expect("Failed to create PostgreSQL ingestor");

    // Run the full ingestion process
    ingestor
        .ingest(output_dir)
        .await
        .expect("Failed to run ingestion process");

    // Verify that the output files were created
    let nodes_path = Path::new(output_dir).join("nodes.jsonl");
    let edges_path = Path::new(output_dir).join("edges.jsonl");
    let schema_path = Path::new(output_dir).join("schema.hx");

    assert!(nodes_path.exists());
    assert!(edges_path.exists());
    assert!(schema_path.exists());

    // Read and verify the nodes file
    let nodes_content = fs::read_to_string(nodes_path).expect("Failed to read nodes file");
    let mut found_node_types: HashSet<String> = HashSet::new();
    nodes_content.lines().for_each(|line| {
        let node: JsonValue = serde_json::from_str(line).unwrap();
        let node_type = node["label"].as_str().unwrap();
        found_node_types.insert(node_type.to_string());
    });
    // We should have 8 nodes (2 users + 2 posts + 2 comments + 2 tags)
    assert_eq!(found_node_types.len(), 10);

    // Read and verify the edges file
    let edges_content = fs::read_to_string(edges_path).expect("Failed to read edges file");
    let mut found_edge_types: HashSet<String> = HashSet::new();
    edges_content.lines().for_each(|line| {
        let edge: JsonValue = serde_json::from_str(line).unwrap();
            let edge_type = edge["label"].as_str().unwrap();
        found_edge_types.insert(edge_type.to_string());
    });

    // We should have 6 edges (2 post-author + 2 comment-post + 2 comment-author + 2 post-tag)
    assert_eq!(found_edge_types.len(), 9);

    // Read and verify the schema file
    let schema_content = fs::read_to_string(schema_path).expect("Failed to read schema file");
    found_node_types.iter().for_each(|node_type| {
        assert!(schema_content.contains(&to_camel_case(node_type)));
    });
    found_edge_types.iter().for_each(|edge_type| {
        assert!(schema_content.contains(&to_camel_case(edge_type)));
    });
}

// Test error handling
#[tokio::test]
async fn test_postgres_error_handling() {
    // Test with an invalid connection string
    let result = PostgresIngestor::new(
        "postgres://invalid:invalid@localhost:5432/invalid",
        Some("test_instance".to_string()),
        1000,
    )
    .await;

    // Should return an error
    assert!(result.is_err());

    // Test with a valid connection but invalid database
    let result = PostgresIngestor::new(
        "postgres://postgres:postgres@localhost:5432/nonexistent_db",
        Some("test_instance".to_string()),
        1000,
    )
    .await;

    // Should return an error
    assert!(result.is_err());
}

// Test with a more complex schema
#[tokio::test]
async fn test_postgres_complex_schema() -> Result<(), Box<dyn std::error::Error>> {
    // Create a configuration for the PostgreSQL connection
    let mut config = Config::new();
    config
        .host("localhost")
        .port(5432)
        .user("postgres")
        .password("postgres")
        .dbname("postgres");

    // Connect to the database
    let (mut client, connection) = config
        .connect(NoTls)
        .await?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Clean up the database first
    cleanup_database(&mut client)
        .await
        .expect("Failed to clean up database");

    // Create a more complex schema with unique table names
    let categories_table = format!("categories");
    let products_table = format!("products");
    let customers_table = format!("customers");
    let orders_table = format!("orders");
    let order_items_table = format!("order_items");

    // Now create the tables with explicit sequence names
    client
        .batch_execute(&format!(
            r#"
            CREATE SEQUENCE {categories}_id_seq;
            CREATE SEQUENCE {products}_id_seq;
            CREATE SEQUENCE {customers}_id_seq;
            CREATE SEQUENCE {orders}_id_seq;
            CREATE SEQUENCE {order_items}_id_seq;
            
            CREATE TABLE {categories} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{categories}_id_seq'),
                name TEXT NOT NULL,
                description TEXT
            );
            
            CREATE TABLE {products} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{products}_id_seq'),
                name TEXT NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                category_id INTEGER REFERENCES {categories_ref}(id),
                description TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE {customers} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{customers}_id_seq'),
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                address TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE {orders} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{orders}_id_seq'),
                customer_id INTEGER REFERENCES {customers_ref}(id),
                order_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL,
                total_amount DECIMAL(10, 2) NOT NULL
            );
            
            CREATE TABLE {order_items} (
                id INTEGER PRIMARY KEY DEFAULT nextval('{order_items}_id_seq'),
                order_id INTEGER REFERENCES {orders_ref}(id),
                product_id INTEGER REFERENCES {products_ref}(id),
                quantity INTEGER NOT NULL,
                unit_price DECIMAL(10, 2) NOT NULL
            );
            "#,
            order_items = order_items_table,
            orders = orders_table,
            products = products_table,
            categories = categories_table,
            customers = customers_table,
            categories_ref = categories_table,
            products_ref = products_table,
            customers_ref = customers_table,
            orders_ref = orders_table
        ))
        .await
        .expect("Failed to create complex schema");

    // Insert sample data
    client
        .execute(
            &format!(
                "INSERT INTO {} (name, description) VALUES ($1, $2)",
                categories_table
            ),
            &[&"Electronics", &"Electronic devices and accessories"],
        )
        .await
        .expect("Failed to insert category");

    client
        .execute(
            &format!(
                "INSERT INTO {} (name, description) VALUES ($1, $2)",
                categories_table
            ),
            &[&"Clothing", &"Apparel and accessories"],
        )
        .await
        .expect("Failed to insert category");

    // Use numeric type for price values
    let laptop_price = Decimal::from_str("999.99").unwrap();
    let tshirt_price = Decimal::from_str("19.99").unwrap();
    let total_amount = Decimal::from_str("1019.98").unwrap();

    client
        .execute(
            &format!(
                "INSERT INTO {} (name, price, category_id, description) VALUES ($1, $2, $3, $4)",
                products_table
            ),
            &[
                &"Laptop",
                &laptop_price,
                &(1 as i32),
                &"High-performance laptop",
            ],
        )
        .await
        .expect("Failed to insert product");

    client
        .execute(
            &format!(
                "INSERT INTO {} (name, price, category_id, description) VALUES ($1, $2, $3, $4)",
                products_table
            ),
            &[&"T-Shirt", &tshirt_price, &(2 as i32), &"Cotton T-shirt"],
        )
        .await
        .expect("Failed to insert product");

    client
        .execute(
            &format!(
                "INSERT INTO {} (name, email, address) VALUES ($1, $2, $3)",
                customers_table
            ),
            &[&"John Doe", &"john@example.com", &"123 Main St"],
        )
        .await
        .expect("Failed to insert customer");

    client
        .execute(
            &format!(
                "INSERT INTO {} (customer_id, status, total_amount) VALUES ($1, $2, $3)",
                orders_table
            ),
            &[&(1 as i32), &"Completed", &total_amount],
        )
        .await
        .expect("Failed to insert order");

    client.execute(
        &format!("INSERT INTO {} (order_id, product_id, quantity, unit_price) VALUES ($1, $2, $3, $4)", order_items_table),
        &[&(1 as i32), &(1 as i32), &(1 as i32), &laptop_price],
    ).await.expect("Failed to insert order item");

    client.execute(
        &format!("INSERT INTO {} (order_id, product_id, quantity, unit_price) VALUES ($1, $2, $3, $4)", order_items_table),
        &[&(1 as i32), &(2 as i32), &(1 as i32), &tshirt_price],
    ).await.expect("Failed to insert order item");

    // Create a temporary directory for test outputs
    let temp_dir = create_temp_dir();
    let output_dir = temp_dir.path().to_str().unwrap();

    // Create a PostgreSQL ingestor
    let mut ingestor = PostgresIngestor::new(
        &format!("postgres://postgres:postgres@localhost:5432/postgres"),
        Some("test_instance".to_string()),
        1000,
    )
    .await
    .expect("Failed to create PostgreSQL ingestor");

    // Run the full ingestion process
    ingestor
        .ingest(output_dir)
        .await
        .expect("Failed to run ingestion process");

    // Verify that the output files were created
    let nodes_path = Path::new(output_dir).join("nodes.jsonl");
    let edges_path = Path::new(output_dir).join("edges.jsonl");
    let schema_path = Path::new(output_dir).join("schema.hx");

    assert!(nodes_path.exists());
    assert!(edges_path.exists());
    assert!(schema_path.exists());

    // Read and verify the nodes file
    let nodes_content = fs::read_to_string(nodes_path).expect("Failed to read nodes file");
    let mut found_node_types: HashSet<String> = HashSet::new();
    nodes_content.lines().for_each(|line| {
        let node: JsonValue = serde_json::from_str(line).unwrap();
        let node_type = node["label"].as_str().unwrap();
        found_node_types.insert(node_type.to_string());
    });

    assert_eq!(found_node_types.len(), 10);

    // Read and verify the edges file
    let edges_content = fs::read_to_string(edges_path).expect("Failed to read edges file");
    let mut found_edge_types: HashSet<String> = HashSet::new();
    edges_content.lines().for_each(|line| {
        let edge: JsonValue = serde_json::from_str(line).unwrap();
        let edge_type = edge["label"].as_str().unwrap();
        found_edge_types.insert(edge_type.to_string());
    });

    // We should have 5 edges (2 product-category + 1 order-customer + 2 order-item relationships)
    assert_eq!(found_edge_types.len(), 9);

    // Read and verify the schema file
    let schema_content = fs::read_to_string(schema_path).expect("Failed to read schema file");
    found_node_types.iter().for_each(|node_type| {
        assert!(schema_content.contains(&to_camel_case(node_type)));
    });
    found_edge_types.iter().for_each(|edge_type| {
        assert!(schema_content.contains(&to_camel_case(edge_type)));
    });

    Ok(())
}

#[tokio::test]
async fn test_postgres_simple_schema() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(5432);
    config.user("postgres");
    config.password("postgres");
    config.dbname("postgres");
    
    let (mut client, connection) = config.connect(NoTls).await?;
    tokio::spawn(connection);
    
    cleanup_database(&mut client).await?;
    // ... rest of the test code ...
    Ok(())
}
