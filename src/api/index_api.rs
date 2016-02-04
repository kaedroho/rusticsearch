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


pub fn view_get_index(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Find index
    let index = match indices.get(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    let mut response = Response::with((status::Ok, "{}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_put_index(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);

    // URL parameters
    let ref index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Load data from body
    let data = json_from_request_body!(req);

    // Create index
    let mut index_path = glob.indices_path.clone();
    index_path.push(index_name);
    index_path.set_extension("rsi");
    let mut index = Index::new(Connection::open(index_path).unwrap());
    index.initialise();
    indices.insert(index_name.clone().to_owned(), index);

    info!("Created index {}", index_name);

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_delete_index(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);

    // URL parameters
    let ref index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

    // Make sure the index exists
    if !glob.indices.read().unwrap().contains_key(index_name.to_owned()) {
        return Ok(index_not_found_response());
    }

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Remove index from array
    indices.remove(index_name.to_owned());

    // Delete file
    let mut index_path = glob.indices_path.clone();
    index_path.push(index_name);
    index_path.set_extension("rsi");
    fs::remove_file(&index_path).unwrap();

    info!("Deleted index {}", index_name);

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_post_refresh_index(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}
