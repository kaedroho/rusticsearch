use std::io::Read;
use std::collections::HashMap;

use rustc_serialize::json;

use system::System;

use api::persistent;
use api::iron::prelude::*;
use api::iron::status;
use api::router::Router;
use api::utils::json_response;


pub fn view_get_global_alias(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref alias_name = read_path_parameter!(req, "alias").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

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
        return Ok(json_response(status::Ok, json::encode(&found_aliases).unwrap()));
    } else {
        return Ok(json_response(status::NotFound, "{}"));
    }
}


pub fn view_get_alias_list(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // TODO

    return Ok(json_response(status::Ok, "{}"));
}

pub fn view_get_alias(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref alias_name = read_path_parameter!(req, "alias").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    // Find alias
    if index.aliases.contains(*alias_name) {
        return Ok(json_response(status::Ok, "{}"));
    } else {
        return Ok(json_response(status::NotFound, "{}"));
    }
}


pub fn view_put_alias(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref alias_name = read_path_parameter!(req, "alias").unwrap_or("");

    // Lock index array
    let mut indices = system.indices.write().unwrap();

    // Get index
    let mut index = get_index_or_404_mut!(indices, *index_name);

    // Insert alias
    index.aliases.insert(alias_name.clone().to_owned());

    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}
