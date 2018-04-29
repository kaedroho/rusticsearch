use std::fs;
use std::io::Read;

use serde_json;
use search::backends::rocksdb::RocksDBStore;
use uuid::Uuid;

use index::Index;
use index::metadata::IndexMetadata;
use index::metadata::parse::parse as parse_index_metadata;

use api::persistent;
use api::iron::prelude::*;
use api::iron::status;
use api::router::Router;
use api::utils::json_response;


pub fn view_get_index(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Get index
    let cluster_metadata = system.metadata.read().unwrap();
    let index = get_index_or_404!(cluster_metadata, *index_name);

    // Serialise index metadata
    let json = {
        match serde_json::to_value(&index.metadata) {
            Ok(json) => json,
            Err(_) => {
                return Ok(json_response(status::InternalServerError, json!({
                    "message": "unable to serialise index metadata"
                })));
            }
        }
    };

    return Ok(json_response(status::Ok, json));
}


pub fn view_put_index(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock cluster metadata
    let mut cluster_metadata = system.metadata.write().unwrap();

    // Find index
    let index_ref = cluster_metadata.names.find_canonical(&index_name);

    match index_ref {
        Some(_) => {
            // Update existing index
            // TODO

            info!(system.log, "updated index"; "index" => *index_name);
        }
        None => {
            // Load metadata
            let mut metadata = IndexMetadata::default();
            match json_from_request_body!(req).map(|data| parse_index_metadata(&mut metadata, data)) {
                Some(Ok(())) | None => {}
                Some(Err(_)) => {
                    // TODO: better error
                    return Ok(json_response(status::BadRequest, json!({"message": "Couldn't parse index settings"})));
                }
            }

            // Create index
            let mut indices_dir = system.get_indices_dir();
            indices_dir.push(index_name);
            let index = Index::new(Uuid::new_v4(), index_name.clone().to_owned(), metadata, RocksDBStore::create(indices_dir).unwrap());
            index.metadata.read().unwrap().save(index.metadata_path()).unwrap();
            let index_ref = cluster_metadata.insert_index(index);

            // If there's an alias with the new indexes name, delete it.
            let alias_deleted = cluster_metadata.names.delete_alias_whole(index_name).unwrap();
            if alias_deleted {
                info!(system.log, "deleted alias"; "alias" => format!("{}", index_name), "reason" => "replaced by index");
            }

            // Register canonical name
            cluster_metadata.names.insert_canonical(index_name.clone().to_owned(), index_ref).unwrap();

            info!(system.log, "created index"; "index" => *index_name);
        }
    }

    return Ok(json_response(status::Ok, json!({"acknowledged": true})));
}


pub fn view_delete_index(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_selector = read_path_parameter!(req, "index").unwrap_or("");

    // Lock cluster metadata
    let mut cluster_metadata = system.metadata.write().unwrap();

    // Make sure the index exists
    get_index_or_404!(cluster_metadata, *index_selector);

    // Remove indices
    for index_ref in cluster_metadata.names.find(*index_selector) {
        // Get the index name
        let index_name = {
            if let Some(index) = cluster_metadata.indices.get(&index_ref) {
                index.canonical_name().to_string()
            } else {
                // Index doesn't exist
                continue;
            }
        };

        // Remove index from array
        cluster_metadata.indices.remove(&index_ref);

        // Delete canonical name
        cluster_metadata.names.delete_canonical(&index_name, index_ref).unwrap();

        // Delete file
        let mut indices_dir = system.get_indices_dir();
        indices_dir.push(&index_name);
        match fs::remove_dir_all(&indices_dir) {
            Ok(()) => {},
            Err(e) => {
                warn!(system.log, "failed to delete index data"; "index" => format!("{}", index_name), "error" => format!("{}", e));
            }
        }

        info!(system.log, "deleted index"; "index" => index_name);

        // Delete aliases
        let alias_names = cluster_metadata.names.iter_index_aliases(index_ref).map(|n| n.to_string()).collect::<Vec<String>>();
        for alias_name in alias_names {
            let alias_deleted = cluster_metadata.names.delete_alias(&alias_name, index_ref).unwrap();

            // If this was the only index being referenced by the alias, the alias would be deleted
            if alias_deleted {
                info!(system.log, "deleted alias"; "alias" => format!("{}", alias_name), "reason" => "no indices left");
            }
        }
    }

    return Ok(json_response(status::Ok, json!({"acknowledged": true})));
}


pub fn view_post_refresh_index(_req: &mut Request) -> IronResult<Response> {
    // let ref system = get_system!(req);
    // let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    // TODO
    // let mut indices = system.indices.write().unwrap();

    // TODO: {"_shards":{"total":10,"successful":5,"failed":0}}
    return Ok(json_response(status::Ok, json!({"acknowledged": true})));
}
