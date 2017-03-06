use std::collections::HashMap;

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

    for index_ref in indices.names.find(alias_name) {
        let index = match indices.get(&index_ref) {
            Some(index) => index,
            None => continue,
        };

        let mut inner_map = HashMap::new();
        let mut inner_inner_map = HashMap::new();
        inner_inner_map.insert(alias_name, HashMap::<String, String>::new());
        inner_map.insert("aliases".to_owned(), inner_inner_map);
        found_aliases.insert(index.canonical_name().clone(), inner_map);
    }

    if !found_aliases.is_empty() {
        return Ok(json_response(status::Ok, json!(found_aliases)));
    } else {
        return Ok(json_response(status::NotFound, json!({})));
    }
}


pub fn view_get_alias_list(_req: &mut Request) -> IronResult<Response> {
    // let ref system = get_system!(req);
    // let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // TODO

    return Ok(json_response(status::Ok, json!({})));
}

pub fn view_get_alias(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref alias_name = read_path_parameter!(req, "alias").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

    // Get index
    let index_ref = match indices.names.find_canonical(index_name) {
        Some(index_ref) => index_ref,
        None => return Ok(json_response(status::NotFound, json!({}))),
    };

    // Find alias
    if indices.names.iter_index_aliases(index_ref).any(|name| &name == alias_name) {
        return Ok(json_response(status::Ok, json!({})));
    } else {
        return Ok(json_response(status::NotFound, json!({})));
    }
}


pub fn view_put_alias(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_selector = read_path_parameter!(req, "index").unwrap_or("");
    let ref alias_name = read_path_parameter!(req, "alias").unwrap_or("");

    // Lock index array
    let mut indices = system.indices.write().unwrap();

    // Insert alias into names registry
    let index_refs = indices.names.find(*index_selector);
    match indices.names.insert_or_replace_alias(alias_name.to_string(), index_refs) {
        Ok(true) => {
            system.log.info("[api] created alias", b!("index" => *index_selector, "alias" => *alias_name));
        }
        Ok(false) => {
            system.log.info("[api] updated alias", b!("index" => *index_selector, "alias" => *alias_name));
        }
        Err(_) => {
            // TODO
            return Ok(json_response(status::Ok, json!({"acknowledged": false})));
        }
    }

    Ok(json_response(status::Ok, json!({"acknowledged": true})))
}
