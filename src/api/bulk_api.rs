use std::io::Read;
use std::collections::HashMap;

use serde_json;

use document::DocumentSource;

use api::persistent;
use api::iron::prelude::*;
use api::iron::status;
use api::utils::{json_response};
use api::router::Router;


pub fn view_post_bulk(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);

    // Lock cluster metedata
    let cluster_metadata = system.metadata.read().unwrap();

    // Load data from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

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
        let action_json = parse_json!(&action_line.unwrap());

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
                let doc_json = parse_json!(&doc_line.unwrap());;

                // Find index
                let index = get_index_or_404!(cluster_metadata, doc_index);
                let index_metadata = index.metadata.read().unwrap();

                let doc = {
                    // Find mapping
                    let mapping = match index_metadata.mappings.get(doc_type) {
                        Some(mapping) => mapping,
                        None => {
                            return Ok(json_response(status::NotFound, json!({"message": "Mapping not found"})));
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
                warn!(system.log, "unrecognised action! {}", action_name);
            }
        }
    }

    return Ok(json_response(status::Ok,
                            json!({
                                "took": items.len(),
                                "items": items,
                            })));
}


pub fn view_post_index_bulk(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock cluster metedata
    let cluster_metadata = system.metadata.read().unwrap();

    // Get index
    let index = get_index_or_404!(cluster_metadata, *index_name);
    let index_metadata = index.metadata.read().unwrap();

    // Load data from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

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
        let action_json = parse_json!(&action_line.unwrap());

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

        match action_name.as_ref() {
            "index" => {
                let doc_line = payload_lines.next();
                let doc_json = parse_json!(&doc_line.unwrap());;

                let doc = {
                    // Find mapping
                    let mapping = match index_metadata.mappings.get(doc_type) {
                        Some(mapping) => mapping,
                        None => {
                            return Ok(json_response(status::NotFound, json!({"message": "Mapping not found"})));
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
                warn!(system.log, "unrecognised action! {}", action_name);
            }
        }
    }

    return Ok(json_response(status::Ok,
                            json!({
                                "took": items.len(),
                                "items": items,
                            })));
}
