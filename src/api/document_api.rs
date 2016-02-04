use std::io::Read;
use std::collections::HashMap;
use std::fs;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::{self, Json};
use rusqlite::Connection;

use super::{persistent, index_not_found_response};
use super::super::{Globals, Index, mapping, Document, query};


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
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Mapping not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    // Find document
    let doc = match index.docs.get(*doc_id) {
        Some(doc) => doc,
        None => {
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Document not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    return json_response!(status::Ok, json::encode(&doc.data).unwrap());
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
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Mapping not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    // Load data from body
    let data = json_from_request_body!(req);

    // Create and insert document
    if let Some(data) = data {
        let doc = Document::from_json(data);
        index.docs.insert(doc_id.clone().to_owned(), doc);
    }

    // TODO: {"_index":"wagtail","_type":"searchtests_searchtest","_id":"searchtests_searchtest:5378","_version":1,"created":true}
    return json_response!(status::Ok, "{}");
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
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Mapping not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    // Make sure the document exists
    if !index.docs.contains_key(*doc_id) {
        let mut response = Response::with((status::NotFound,
                                           "{\"message\": \"Document not found\"}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        return Ok(response);
    }

    // Delete document
    index.docs.remove(*doc_id);

    return json_response!(status::Ok, "{}");
}
