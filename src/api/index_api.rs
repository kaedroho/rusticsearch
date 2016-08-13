use std::io::Read;

use rustc_serialize::json::Json;

use system::index::Index;
use search::store::memory::MemoryIndexStore;

use api::persistent;
use api::iron::prelude::*;
use api::iron::status;
use api::router::Router;
use api::utils::json_response;


pub fn view_get_index(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    return Ok(json_response(status::Ok, "{}"));
}


pub fn view_put_index(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let mut indices = system.indices.write().unwrap();

    // Load data from body
    // let data = json_from_request_body!(req);

    // Create index
    let mut indices_dir = system.get_indices_dir();
    indices_dir.push(index_name);
    indices_dir.set_extension("rsi");
    let mut index = Index::new(MemoryIndexStore::new());
    indices.insert(index_name.clone().to_owned(), index);

    // TODO: load settings

    system.log.info("[api] created index", b!("index" => *index_name));

    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}


pub fn view_delete_index(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Make sure the index exists
    get_index_or_404!(system.indices.read().unwrap(), *index_name);

    // Lock index array
    let mut indices = system.indices.write().unwrap();

    // Remove index from array
    indices.remove(index_name.to_owned());

    // Delete file
    let mut indices_dir = system.get_indices_dir();
    indices_dir.push(index_name);
    indices_dir.set_extension("rsi");
    // fs::remove_file(&index_path).unwrap();

    system.log.info("[api] deleted index", b!("index" => *index_name));

    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}


pub fn view_post_refresh_index(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let mut indices = system.indices.write().unwrap();

    // TODO: {"_shards":{"total":10,"successful":5,"failed":0}}
    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}
