use std::io::Read;
use std::collections::BTreeMap;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::{self, Json};

use super::persistent;
use super::utils::json_response;
use super::super::{Globals, Document};


pub fn view_get_doc(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");
    let ref doc_id = read_path_parameter!(req, "doc").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    // Find mapping
    let mapping = match index.mappings.get(*mapping_name) {
        Some(mapping) => mapping,
        None => {
            return Ok(json_response(status::NotFound, "{\"message\": \"Mapping not found\"}"));
        }
    };

    // Find document
    let doc = match index.docs.get(*doc_id) {
        Some(doc) => doc,
        None => {
            return Ok(json_response(status::NotFound, "{\"message\": \"Document not found\"}"));
        }
    };


    // Build JSON document
    let mut json_object = BTreeMap::new();
    for (field_name, field_value) in doc.fields.iter() {
        json_object.insert(field_name.clone(), field_value.as_json());
    }

    let json = Json::Object(json_object);
    return Ok(json_response(status::Ok, json::encode(&json).unwrap()));
}


pub fn view_put_doc(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");
    let ref doc_id = read_path_parameter!(req, "doc").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Get index
    let mut index = get_index_or_404_mut!(indices, *index_name);

    // Find mapping
    let mut mapping = match index.mappings.get_mut(*mapping_name) {
        Some(mapping) => mapping,
        None => {
            return Ok(json_response(status::NotFound, "{\"message\": \"Mapping not found\"}"));
        }
    };

    // Load data from body
    let data = json_from_request_body!(req);

    // Create and insert document
    if let Some(data) = data {
        let doc = Document::from_json(data, mapping);
        index.docs.insert(doc_id.clone().to_owned(), doc);
    }

    // TODO: {"_index":"wagtail","_type":"searchtests_searchtest","_id":"searchtests_searchtest:5378","_version":1,"created":true}
    return Ok(json_response(status::Ok, "{}"));
}


pub fn view_delete_doc(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");
    let ref doc_id = read_path_parameter!(req, "doc").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Get index
    let mut index = get_index_or_404_mut!(indices, *index_name);

    // Find mapping
    let mut mapping = match index.mappings.get_mut(*mapping_name) {
        Some(mapping) => mapping,
        None => {
            return Ok(json_response(status::NotFound, "{\"message\": \"Mapping not found\"}"));
        }
    };

    // Make sure the document exists
    if !index.docs.contains_key(*doc_id) {
        return Ok(json_response(status::NotFound, "{\"message\": \"Document not found\"}"));
    }

    // Delete document
    index.docs.remove(*doc_id);

    return Ok(json_response(status::Ok, "{}"));
}
