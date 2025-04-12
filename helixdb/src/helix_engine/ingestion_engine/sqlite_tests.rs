use crate::helix_engine::ingestion_engine::sqlite::{SqliteIngestor, ColumnInfo};

#[test]
fn test_ingest_real_db() {
    let mut ingestor = SqliteIngestor::new("../data/dummy_data.sqlite", None, 5).unwrap();
    let schemas = ingestor.extract_schema().unwrap();
    println!("{:?}", schemas);
}
