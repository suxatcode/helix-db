use std::sync::Arc;
use crate::HelixEmbedded;
use crate::GraphError;
use crate::HelixLiteError;

pub struct Helix {
    db: HelixEmbedded,
}

impl Helix {
    pub fn new(path: String) -> Result<Self, HelixLiteError> {
        Ok(Self {
            db: HelixEmbedded::new(path)?
        })
    }

    pub fn execute_query(&self, query_id: String, json_body: String) -> Result<String, HelixLiteError> {
        self.db.query(query_id, json_body)
    }
}