use std::sync::Arc;

use serde_json::Value;
use rocket::State;
use rocket_contrib::JSON;

use system::System;
use document::DocumentSource;


#[allow(unused_variables)]
#[get("/<index_name>/<mapping_name>/<doc_key>")]
pub fn get(index_name: &str, mapping_name: &str, doc_key: &str, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Get index
    let cluster_metadata = system.metadata.read().unwrap();
    let index = get_index_or_404!(cluster_metadata, index_name);
    let index_metadata = index.metadata.read().unwrap();

    // Check that the mapping exists
    if !index_metadata.mappings.contains_key(mapping_name) {
        return None;
    }

    // Find document
    /*
    let index_reader = index.store.reader();
    let doc = match index_reader.get_document_by_key(doc_key) {
        Some(doc) => doc,
        None => {
            return Ok(json_response(status::NotFound, "{\"message\": \"Document not found\"}"));
        }
    };
    */


    // Build JSON document
    // TODO: This is probably completely wrong
    // let json_object = BTreeMap::new();
    // FIXME: for (field_name, field_value) in doc.fields.iter() {
    // FIXME:     json_object.insert(field_name.clone(), Json::Array(field_value.iter().map(|v| v.term.as_json()).collect::<Vec<_>>()));
    // FIXME: }

    Some(JSON(json!({})))
}


#[put("/<index_name>/<mapping_name>/<doc_key>", data = "<data>")]
pub fn put(index_name: &str, mapping_name: &str, doc_key: &str, data: JSON<Value>, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Get index
    let cluster_metadata = system.metadata.read().unwrap();
    let index = get_index_or_404!(cluster_metadata, index_name);
    let index_metadata = index.metadata.read().unwrap();

    let doc = {
        // Find mapping
        let mapping = match index_metadata.mappings.get(mapping_name) {
            Some(mapping) => mapping,
            None => {
                return None;
            }
        };

        // Create document
        let document_source = DocumentSource {
            key: doc_key,
            data: data.as_object().unwrap(),
        };
        document_source.prepare(mapping).unwrap()
    };

    index.store.insert_or_update_document(&doc).unwrap();

    // TODO: {"_index":"wagtail","_type":"searchtests_searchtest","_id":"searchtests_searchtest:5378","_version":1,"created":true}
    Some(JSON(json!({})))
}


#[delete("/<index_name>/<mapping_name>/<doc_key>")]
pub fn delete(index_name: &str, mapping_name: &str, doc_key: &str, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Get index
    let cluster_metadata = system.metadata.read().unwrap();
    let index = get_index_or_404!(cluster_metadata, index_name);
    let index_metadata = index.metadata.read().unwrap();

    // Check that the mapping exists
    if !index_metadata.mappings.contains_key(mapping_name) {
        return None;
    }

    // Make sure the document exists
    if !index.store.reader().contains_document_key(doc_key) {
        return None;
    }

    // Delete document
    index.store.remove_document_by_key(doc_key).unwrap();

    Some(JSON(json!({})))
}
