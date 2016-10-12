use std::collections::BTreeMap;

use rustc_serialize::json::Json;

use document::Document;


#[derive(Debug)]
pub struct SearchHit<'a> {
    pub doc: &'a Document,
    pub score: f64,
}


impl<'a> SearchHit<'a> {
    pub fn as_json(&self) -> Json {
        let mut pk_field: Vec<Json> = Vec::new();
        pk_field.push(self.doc.indexed_fields.get("pk").unwrap()[0].term.as_json());

        let mut fields = BTreeMap::new();
        fields.insert("pk".to_owned(), Json::Array(pk_field));

        let mut hit = BTreeMap::new();
        hit.insert("_score".to_owned(), Json::F64(self.score));
        hit.insert("fields".to_owned(), Json::Object(fields));
        Json::Object(hit)
    }
}


#[derive(Debug)]
pub struct SearchResponse<'a> {
    pub total_hits: usize,
    pub hits: Vec<SearchHit<'a>>,
    pub terminated_early: bool,
}
