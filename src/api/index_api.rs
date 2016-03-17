use std::io::Read;
use std::fs;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::Json;

use super::persistent;
use super::utils::json_response;
use super::super::{Globals, Index};


pub fn view_get_index(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    return Ok(json_response(status::Ok, "{}"));
}


pub fn view_put_index(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Load data from body
    let data = json_from_request_body!(req);

    // Create index
    let mut index_path = glob.indices_path.clone();
    index_path.push(index_name);
    index_path.set_extension("rsi");
    let mut index = Index::new();
    index.initialise();
    indices.insert(index_name.clone().to_owned(), index);

    info!("Created index {}", index_name);

    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}


pub fn view_delete_index(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Make sure the index exists
    get_index_or_404!(glob.indices.read().unwrap(), *index_name);

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

    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}


pub fn view_post_refresh_index(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // TODO: {"_shards":{"total":10,"successful":5,"failed":0}}
    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}
