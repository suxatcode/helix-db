use std::io::Result;
use napi_derive::napi;
use crate::HelixEmbedded;

#[napi]
pub struct HelixJS {
    db: HelixEmbedded,
}

#[napi]
impl HelixJS {
    #[napi(constructor)]
    pub fn new(user: String) -> napi::Result<HelixJS> {
        match HelixEmbedded::new(user) {
            Ok(db) => Ok(Self { db }),
            Err(e) => Err(napi::Error::from_reason(e.to_string()))
        }
    }

    #[napi]
    pub fn query(&self, query_id: String, json_body: String) -> napi::Result<String> {
        match self.db.query(query_id, json_body) {
            Ok(result) => Ok(result),
            Err(e) => Err(napi::Error::from_reason(e.to_string()))
        }
    }
}