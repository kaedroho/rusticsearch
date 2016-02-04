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

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
    let mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");

    let doc_id = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Find index
    let index = match indices.get(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Find mapping
    let mapping = match index.mappings.get(mapping_name) {
        Some(mapping) => mapping,
        None => {
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Mapping not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    // Find document
    let doc = match index.docs.get(doc_id) {
        Some(doc) => doc,
        None => {
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Document not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    let mut response = Response::with((status::Ok, json::encode(&doc.data).unwrap()));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_put_doc(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
    let mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");
    let ref doc_id = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Find index
    let mut index = match indices.get_mut(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Find mapping
    let mut mapping = match index.mappings.get_mut(mapping_name) {
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

    let mut response = Response::with((status::Ok, "{}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_delete_doc(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
    let mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");
    let doc_id = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Find index
    let mut index = match indices.get_mut(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Find mapping
    let mut mapping = match index.mappings.get_mut(mapping_name) {
        Some(mapping) => mapping,
        None => {
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Mapping not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    // Make sure the document exists
    if !index.docs.contains_key(doc_id.clone()) {
        let mut response = Response::with((status::NotFound,
                                           "{\"message\": \"Document not found\"}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        return Ok(response);
    }

    // Delete document
    index.docs.remove(doc_id);

    let mut response = Response::with((status::Ok, "{}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}
