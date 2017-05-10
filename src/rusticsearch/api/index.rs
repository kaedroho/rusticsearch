use std::fs;
use std::sync::Arc;

use serde_json::Value;
use serde_json::value::ToJson;
use kite_rocksdb::RocksDBIndexStore;
use uuid::Uuid;
use rocket::State;
use rocket_contrib::JSON;

use system::System;
use index::Index;
use index::metadata::IndexMetadata;
use index::metadata::parse::parse as parse_index_metadata;


#[get("/<index_name>")]
pub fn get(index_name: &str, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Get index
    let cluster_metadata = system.metadata.read().unwrap();
    let index = get_index_or_404!(cluster_metadata, index_name);

    // Serialise index metadata
    let json = {
        let index_metadata = index.metadata.read().unwrap();
        match index_metadata.to_json() {
            Ok(json) => json,
            Err(_) => {
                return Some(JSON(json!({"message": "unable to serialise index metadata"})));  // TODO 500 error
            }
        }
    };

    Some(JSON(json))
}


#[put("/<index_name>", data = "<data>")]
pub fn put(index_name: &str, data: JSON<Value>, system: State<Arc<System>>) -> JSON<Value> {
    // Lock cluster metadata
    let mut cluster_metadata = system.metadata.write().unwrap();

    // Find index
    let index_ref = cluster_metadata.names.find_canonical(&index_name);

    match index_ref {
        Some(_) => {
            // Update existing index
            // TODO

            system.log.info("[api] updated index", b!("index" => index_name));
        }
        None => {
            // Load metadata
            let mut metadata = IndexMetadata::default();

            match parse_index_metadata(&mut metadata, data.into_inner()) {
                Ok(()) => {}
                Err(_) => {
                    return JSON(json!({"message": "Couldn't parse index settings"}));  // TODO 400 error
                }
            }

            // Create index
            let mut indices_dir = system.get_indices_dir();
            indices_dir.push(index_name);
            let index = Index::new(Uuid::new_v4(), index_name.clone().to_owned(), metadata, RocksDBIndexStore::create(indices_dir).unwrap());
            index.metadata.read().unwrap().save(index.metadata_path()).unwrap();
            let index_ref = cluster_metadata.insert_index(index);

            // If there's an alias with the new indexes name, delete it.
            let alias_deleted = cluster_metadata.names.delete_alias_whole(index_name).unwrap();
            if alias_deleted {
                 system.log.info("[api] deleted alias", b!("alias" => format!("{}", index_name), "reason" => "replaced by index"));
            }

            // Register canonical name
            cluster_metadata.names.insert_canonical(index_name.clone().to_owned(), index_ref).unwrap();

            system.log.info("[api] created index", b!("index" => index_name));
        }
    }

    JSON(json!({"acknowledged": true}))
}


#[delete("/<index_selector>")]
pub fn delete(index_selector: &str, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Lock cluster metadata
    let mut cluster_metadata = system.metadata.write().unwrap();

    // Make sure the index exists
    get_index_or_404!(cluster_metadata, index_selector);

    // Remove indices
    for index_ref in cluster_metadata.names.find(index_selector) {
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
                system.log.warn("[api] failed to delete index data", b!("index" => format!("{}", index_name), "error" => format!("{}", e)));
            }
        }

        system.log.info("[api] deleted index", b!("index" => format!("{}", index_name)));

        // Delete aliases
        let alias_names = cluster_metadata.names.iter_index_aliases(index_ref).map(|n| n.to_string()).collect::<Vec<String>>();
        for alias_name in alias_names {
            let alias_deleted = cluster_metadata.names.delete_alias(&alias_name, index_ref).unwrap();

            // If this was the only index being referenced by the alias, the alias would be deleted
            if alias_deleted {
                 system.log.info("[api] deleted alias", b!("alias" => format!("{}", alias_name), "reason" => "no indices left"));
            }
        }
    }

    Some(JSON(json!({"acknowledged": true})))
}


#[allow(unused_variables)]
#[post("/<index_name>/_refresh")]
pub fn refresh(index_name: &str) -> Option<JSON<Value>> {
    // Lock index array
    // TODO
    // let mut indices = system.indices.write().unwrap();

    // TODO: {"_shards":{"total":10,"successful":5,"failed":0}}
    Some(JSON(json!({"acknowledged": true})))
}
