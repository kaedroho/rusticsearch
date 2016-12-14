use std::io::Read;
use std::collections::BTreeMap;

use rustc_serialize::json::{self, Json};

use document::DocumentSource;

use api::persistent;
use api::iron::prelude::*;
use api::iron::status;
use api::router::Router;
use api::utils::json_response;


pub fn view_get_doc(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");
    // let ref doc_key = read_path_parameter!(req, "doc").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    // Check that the mapping exists
    if index.get_mapping_by_name(mapping_name).is_none() {
        return Ok(json_response(status::NotFound, "{\"message\": \"Mapping not found\"}"));
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
    let json_object = BTreeMap::new();
    // FIXME: for (field_name, field_value) in doc.fields.iter() {
    // FIXME:     json_object.insert(field_name.clone(), Json::Array(field_value.iter().map(|v| v.term.as_json()).collect::<Vec<_>>()));
    // FIXME: }

    let json = Json::Object(json_object);
    return Ok(json_response(status::Ok, json::encode(&json).unwrap()));
}


pub fn view_put_doc(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");
    let ref doc_key = read_path_parameter!(req, "doc").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    let doc = {
        // Find mapping
        let mapping = match index.get_mapping_by_name(mapping_name) {
            Some(mapping) => mapping,
            None => {
                return Ok(json_response(status::NotFound, "{\"message\": \"Mapping not found\"}"));
            }
        };

        // Create document
        if let Some(data) = json_from_request_body!(req) {
            let document_source = DocumentSource {
                key: doc_key.to_string(),
                data: data,
            };
            document_source.prepare(mapping)
        } else {
            return Ok(json_response(status::NotFound, "{\"message\": \"No data\"}"));
        }
    };

    index.store.insert_or_update_document(doc).unwrap();

    // TODO: {"_index":"wagtail","_type":"searchtests_searchtest","_id":"searchtests_searchtest:5378","_version":1,"created":true}
    return Ok(json_response(status::Ok, "{}"));
}


pub fn view_delete_doc(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");
    let ref doc_key = read_path_parameter!(req, "doc").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    // Check that the mapping exists
    if index.get_mapping_by_name(mapping_name).is_none() {
        return Ok(json_response(status::NotFound, "{\"message\": \"Mapping not found\"}"));
    }

    // Make sure the document exists
    if !index.store.reader().contains_document_key(doc_key) {
        return Ok(json_response(status::NotFound, "{\"message\": \"Document not found\"}"));
    }

    // Delete document
    index.store.remove_document_by_key(doc_key).unwrap();

    return Ok(json_response(status::Ok, "{}"));
}
