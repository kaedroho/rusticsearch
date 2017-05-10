use std::io::Read;
use std::collections::HashMap;

use serde_json;

use document::DocumentSource;

use std::sync::Arc;

use serde_json::Value;
use rocket::State;
use rocket::Data;
use rocket_contrib::JSON;

use system::System;


#[post("/_bulk", data = "<data>")]
pub fn bulk(data: Data, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Lock cluster metedata
    let cluster_metadata = system.metadata.read().unwrap();

    // Load data from body
    let mut payload = String::new();
    data.open().read_to_string(&mut payload).unwrap();

    let mut items = Vec::new();

    // Iterate
    let mut payload_lines = payload.split('\n');
    loop {
        let action_line = payload_lines.next();

        // Check if end of input
        if action_line == None || action_line == Some("") {
            break;
        }

        // Parse action line
        let action_json: Value = match serde_json::from_str(&action_line.unwrap()) {
            Ok(data) => data,
            Err(_) => {
                // TODO: Don't stop the bulk upload here
                return None;
            }
        };

        // Check action
        // Action should be an object with only one key, the key name indicates the action and
        // the value is the parameters for that action
        let action_name = action_json.as_object().unwrap().keys().nth(0).unwrap();
        let action_params = action_json.as_object()
                                       .unwrap()
                                       .get(action_name)
                                       .unwrap()
                                       .as_object()
                                       .unwrap();

        let doc_id = action_params.get("_id").unwrap().as_str().unwrap();
        let doc_type = action_params.get("_type").unwrap().as_str().unwrap();
        let doc_index = action_params.get("_index").unwrap().as_str().unwrap();

        match action_name.as_ref() {
            "index" => {
                let doc_line = payload_lines.next();
                let doc_json: Value = match serde_json::from_str(&doc_line.unwrap()) {
                    Ok(data) => data,
                    Err(_) => {
                        // TODO: Don't stop the bulk upload here
                        return None;
                    }
                };

                // Find index
                let index = get_index_or_404!(cluster_metadata, doc_index);
                let index_metadata = index.metadata.read().unwrap();

                let doc = {
                    // Find mapping
                    let mapping = match index_metadata.mappings.get(doc_type) {
                        Some(mapping) => mapping,
                        None => {
                            // TODO: Don't stop the bulk upload here
                            return None;
                        }
                    };

                    // Create document
                    let document_source = DocumentSource {
                        key: doc_id,
                        data: doc_json.as_object().unwrap(),
                    };
                    document_source.prepare(mapping).unwrap()
                };

                index.store.insert_or_update_document(&doc).unwrap();

                // Insert into "items" array
                let mut item = HashMap::new();
                // TODO: "create" may not always be right
                item.insert("create", action_params.clone());
                items.push(item);
            }
            _ => {
                warn!("Unrecognised action! {}", action_name);
            }
        }
    }

    Some(JSON(json!({
        "took": items.len(),
        "items": items,
    })))
}
