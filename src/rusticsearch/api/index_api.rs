use std::fs;
use std::io::Read;

use rustc_serialize::json::Json;
use kite_rocksdb::RocksDBIndexStore;
use uuid::Uuid;

use index::Index;
use index::metadata::IndexMetaData;
use index::metadata::parse::parse as parse_index_metadata;

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

    // Check index exists
    get_index_or_404!(indices, *index_name);

    // TODO
    return Ok(json_response(status::Ok, "{}"));
}


pub fn view_put_index(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let mut indices = system.indices.write().unwrap();

    // Find index
    let index_ref = indices.names.find_canonical(&index_name);

    match index_ref {
        Some(_) => {
            // Update existing index
            // TODO

            system.log.info("[api] updated index", b!("index" => *index_name));
        }
        None => {
            // Load metadata
            let mut metadata = IndexMetaData::default();
            match json_from_request_body!(req).map(|data| parse_index_metadata(&mut metadata, data)) {
                Some(Ok(())) | None => {}
                Some(Err(_)) => {
                    // TODO: better error
                    return Ok(json_response(status::BadRequest, "{\"message\": \"Couldn't parse index settings\"}"));
                }
            }

            // Create index
            let mut indices_dir = system.get_indices_dir();
            indices_dir.push(index_name);
            let index = Index::new(Uuid::new_v4(), index_name.clone().to_owned(), metadata, RocksDBIndexStore::create(indices_dir).unwrap());
            let index_ref = indices.insert(index);

            // If there's an alias with the new indexes name, delete it.
            let alias_deleted = indices.names.delete_alias_whole(index_name).unwrap();
            if alias_deleted {
                 system.log.info("[api] deleted alias", b!("alias" => format!("{}", index_name), "reason" => "replaced by index"));
            }

            // Register canonical name
            indices.names.insert_canonical(index_name.clone().to_owned(), index_ref).unwrap();

            system.log.info("[api] created index", b!("index" => *index_name));
        }
    }

    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}


pub fn view_delete_index(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_selector = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let mut indices = system.indices.write().unwrap();

    // Make sure the index exists
    get_index_or_404!(indices, *index_selector);

    // Remove indices
    for index_ref in indices.names.find(*index_selector) {
        // Get the index name
        let index_name = {
            if let Some(index) = indices.get(&index_ref) {
                index.canonical_name().to_string()
            } else {
                // Index doesn't exist
                continue;
            }
        };

        // Remove index from array
        indices.remove(&index_ref);

        // Delete canonical name
        indices.names.delete_canonical(&index_name, index_ref).unwrap();

        // Delete file
        let mut indices_dir = system.get_indices_dir();
        indices_dir.push(&index_name);
        match fs::remove_dir_all(&indices_dir) {
            Ok(()) => {},
            Err(e) => {
                system.log.warn("[api] failed to delete index data", b!("index" => format!("{}", index_name), "error" => format!("{}", e)));
            }
        }

        system.log.info("[api] deleted index", b!("index" => format!("{}", index_name)));

        // Delete aliases
        let alias_names = indices.names.iter_index_aliases(index_ref).map(|n| n.to_string()).collect::<Vec<String>>();
        for alias_name in alias_names {
            let alias_deleted = indices.names.delete_alias(&alias_name, index_ref).unwrap();

            // If this was the only index being referenced by the alias, the alias would be deleted
            if alias_deleted {
                 system.log.info("[api] deleted alias", b!("alias" => format!("{}", alias_name), "reason" => "no indices left"));
            }
        }
    }

    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}


pub fn view_post_refresh_index(_req: &mut Request) -> IronResult<Response> {
    // let ref system = get_system!(req);
    // let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    // TODO
    // let mut indices = system.indices.write().unwrap();

    // TODO: {"_shards":{"total":10,"successful":5,"failed":0}}
    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}
