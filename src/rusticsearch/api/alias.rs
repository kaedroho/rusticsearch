use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use rocket::State;
use rocket_contrib::JSON;

use system::System;


#[get("/_alias/<alias_name>")]
pub fn get_global(alias_name: &str, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Lock cluster metadata
    let cluster_metadata = system.metadata.read().unwrap();

    // Find alias
    let mut found_aliases = HashMap::new();

    for index_ref in cluster_metadata.names.find(alias_name) {
        let index = match cluster_metadata.indices.get(&index_ref) {
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
        Some(JSON(json!(found_aliases)))
    } else {
        None
    }
}


#[allow(unused_variables)]
#[get("/<index_name>/_alias")]
pub fn get_list(index_name: &str, system: State<Arc<System>>) -> JSON<Value> {
    // TODO

    JSON(json!({}))
}


#[get("/<index_name>/_alias/<alias_name>")]
pub fn get(index_name: &str, alias_name: &str, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Lock cluster metadata
    let cluster_metadata = system.metadata.read().unwrap();

    // Get index
    let index_ref = match cluster_metadata.names.find_canonical(index_name) {
        Some(index_ref) => index_ref,
        None => return None,
    };

    // Find alias
    if cluster_metadata.names.iter_index_aliases(index_ref).any(|name| name == alias_name) {
        Some(JSON(json!({})))
    } else {
        None
    }
}


#[put("/<index_selector>/_alias/<alias_name>")]
pub fn put(index_selector: &str, alias_name: &str, system: State<Arc<System>>) -> JSON<Value> {
    // Lock cluster metadata
    let mut cluster_metadata = system.metadata.write().unwrap();

    // Insert alias into names registry
    let index_refs = cluster_metadata.names.find(index_selector);
    match cluster_metadata.names.insert_or_replace_alias(alias_name.to_string(), index_refs) {
        Ok(true) => {
            system.log.info("[api] created alias", b!("index" => index_selector, "alias" => alias_name));
        }
        Ok(false) => {
            system.log.info("[api] updated alias", b!("index" => index_selector, "alias" => alias_name));
        }
        Err(_) => {
            // TODO
            return JSON(json!({"acknowledged": false}));
        }
    }

    JSON(json!({"acknowledged": true}))
}
