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


pub fn view_get_global_alias(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref alias_name = read_path_parameter!(req, "alias").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Find alias
    let mut found_aliases = HashMap::new();
    for (index_name, index) in indices.iter() {
        if index.aliases.contains(*alias_name) {
            let mut inner_map = HashMap::new();
            let mut inner_inner_map = HashMap::new();
            inner_inner_map.insert(alias_name, HashMap::<String, String>::new());
            inner_map.insert("aliases".to_owned(), inner_inner_map);
            found_aliases.insert(index_name, inner_map);
        }
    }

    if !found_aliases.is_empty() {
        let mut response = Response::with((status::Ok, json::encode(&found_aliases).unwrap()));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    } else {
        let mut response = Response::with((status::NotFound, "{}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    }
}


pub fn view_get_alias(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref alias_name = read_path_parameter!(req, "alias").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Find index
    let index = match indices.get(*index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Find alias
    if index.aliases.contains(*alias_name) {
        let mut response = Response::with((status::Ok, ""));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    } else {
        let mut response = Response::with((status::NotFound, ""));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    }
}


pub fn view_put_alias(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref alias_name = read_path_parameter!(req, "alias").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Find index
    let mut index = match indices.get_mut(*index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Insert alias
    index.aliases.insert(alias_name.clone().to_owned());

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}
