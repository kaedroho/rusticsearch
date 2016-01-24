extern crate router;
extern crate persistent;

use std::io::Read;
use std::collections::HashMap;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::{self, Json};
use rusqlite::Connection;

use super::{Globals, Index, Mapping, Document, query};


fn index_not_found_response() -> Response {
    let mut response = Response::with((status::NotFound, "{\"message\": \"Index not found\"}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    return response;
}


pub fn view_home(_: &mut Request) -> IronResult<Response> {
    Ok(Response::with((status::Ok, "Hello World!")))
}


pub fn view_count(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Find index
    let index = match indices.get(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Load query from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

    let count = if !payload.is_empty() {
        let query_data = match Json::from_str(&payload) {
            Ok(data) => data,
            Err(error) => {
                // TODO: What specifically is bad about the JSON?
                let mut response = Response::with((status::BadRequest,
                                                   "{\"message\": \"Couldn't parse JSON\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        };

        // Parse query
        let query = query::parse_query(query_data.as_object().unwrap().get("query").unwrap());
        debug!("{:#?}", query);

        match query {
            Ok(query) => {
                let mut count = 0;
                for (_, doc) in index.docs.iter() {
                    if query.matches(&doc) {
                        count += 1;
                    }
                }

                count
            }
            Err(error) => {
                // TODO: What specifically is bad about the Query?
                let mut response = Response::with((status::BadRequest,
                                                   "{\"message\": \"Query error\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        }

    } else {
        index.docs.len()
    };

    let mut response = Response::with((status::Ok, format!("{{\"count\": {}}}", count)));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_search(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Find index
    let index = match indices.get(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Load query from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

    let data = if !payload.is_empty() {
        Some(match Json::from_str(&payload) {
            Ok(data) => data,
            Err(error) => {
                // TODO: What specifically is bad about the JSON?
                let mut response = Response::with((status::BadRequest,
                                                   "{\"message\": \"Couldn't parse JSON\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        })
    } else {
        None
    };

    debug!("{:#?}", query::parse_query(data.unwrap().as_object().unwrap().get("query").unwrap()));

    // TODO: Run query

    let mut response = Response::with((status::Ok, "{\"hits\": {\"total\": 0, \"hits\": []}}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_get_doc(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
    let mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");

    let doc_id = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Find index
    let index = match indices.get(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Find mapping
    let mapping = match index.mappings.get(mapping_name) {
        Some(mapping) => mapping,
        None => {
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Mapping not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    // Find document
    let doc = match index.docs.get(doc_id) {
        Some(doc) => doc,
        None => {
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Document not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    let mut response = Response::with((status::Ok, json::encode(&doc.data).unwrap()));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_put_doc(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
    let mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");
    let ref doc_id = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Find index
    let mut index = match indices.get_mut(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Find mapping
    let mut mapping = match index.mappings.get_mut(mapping_name) {
        Some(mapping) => mapping,
        None => {
            let mut response = Response::with((status::NotFound,
                                               "{\"message\": \"Mapping not found\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    // Load data from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

    let data = if !payload.is_empty() {
        Some(match Json::from_str(&payload) {
            Ok(data) => data,
            Err(error) => {
                // TODO: What specifically is bad about the JSON?
                let mut response = Response::with((status::BadRequest,
                                                   "{\"message\": \"Couldn't parse JSON\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        })
    } else {
        None
    };

    // Lock index array
    let mut indices = glob.indices.read().unwrap();

    // Create and insert document
    if let Some(data) = data {
        let doc = Document::from_json(data);
        index.docs.insert(doc_id.clone().to_owned(), doc);
    }

    let mut response = Response::with((status::Ok, "{}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_put_index(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // URL parameters
    let ref index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Load data from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

    let data = if !payload.is_empty() {
        Some(match Json::from_str(&payload) {
            Ok(data) => data,
            Err(error) => {
                // TODO: What specifically is bad about the JSON?
                let mut response = Response::with((status::BadRequest,
                                                   "{\"message\": \"Couldn't parse JSON\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        })
    } else {
        None
    };

    // Create index
    let mut index_path = glob.indices_path.clone();
    index_path.push(index_name);
    index_path.set_extension("rsi");
    let mut index = Index::new(Connection::open(index_path).unwrap());
    index.initialise();
    indices.insert(index_name.clone().to_owned(), index);

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_put_mapping(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
    let ref mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Find index
    let mut index = match indices.get_mut(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    // Load data from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

    if payload.is_empty() {
        // TODO: Better error
        let mut response = Response::with((status::Ok, "{\"acknowledged\": false}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        return Ok(response)
    }

    let data = match Json::from_str(&payload) {
        Ok(data) => data,
        Err(error) => {
            // TODO: What specifically is bad about the JSON?
            let mut response = Response::with((status::BadRequest,
                                               "{\"message\": \"Couldn't parse JSON\"}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response);
        }
    };

    let data = data.as_object().unwrap().get(*mapping_name).unwrap();

    // Insert mapping
    let mapping = Mapping::from_json(&data);
    debug!("{:#?}", mapping);
    index.mappings.insert(mapping_name.clone().to_owned(), mapping);

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_post_bulk(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Load data from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

    let mut items = Vec::new();

    // Iterate
    let mut payload_lines = payload.split('\n');
    loop {
        let action_line = payload_lines.next();

        // Check if end of input
        if action_line == None || action_line == Some("") { break; }

        // Parse action line
        let action_json = match Json::from_str(&action_line.unwrap()) {
            Ok(data) => data,
            Err(error) => {
                // TODO: What specifically is bad about the JSON?
                let mut response = Response::with((status::BadRequest,
                                                   "{\"message\": \"Couldn't parse JSON\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        };

        // Check action
        // Action should be an object with only one key, the key name indicates the action and
        // the value is the parameters for that action
        let action_name = action_json.as_object().unwrap().keys().nth(0).unwrap();
        let action_params = action_json.as_object().unwrap().get(action_name).unwrap()
                                                            .as_object().unwrap();

        let doc_id = action_params.get("_id").unwrap().as_string().unwrap();
        let doc_type = action_params.get("_type").unwrap().as_string().unwrap();
        let doc_index = action_params.get("_index").unwrap().as_string().unwrap();

        match action_name.as_ref() {
            "index" => {
                let doc_line = payload_lines.next();
                let doc_json =  match Json::from_str(&doc_line.unwrap()) {
                    Ok(data) => data,
                    Err(error) => {
                        // TODO: What specifically is bad about the JSON?
                        let mut response = Response::with((status::BadRequest,
                                                           "{\"message\": \"Couldn't parse JSON\"}"));
                        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                        return Ok(response);
                    }
                };

                // Find index
                let mut index = match indices.get_mut(doc_index) {
                    Some(index) => index,
                    None => {
                        return Ok(index_not_found_response());
                    }
                };

                // Find mapping
                let mut mapping = match index.mappings.get_mut(doc_type) {
                    Some(mapping) => mapping,
                    None => {
                        let mut response = Response::with((status::NotFound,
                                                           "{\"message\": \"Mapping not found\"}"));
                        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                        return Ok(response);
                    }
                };

                // Create and insert document
                let doc = Document::from_json(doc_json);
                index.docs.insert(doc_id.clone().to_owned(), doc);

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

    let mut response = Response::with((status::Ok, format!("{{\"took\": {}, \"items\": {}}}", items.len(), json::encode(&items).unwrap())));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_post_refresh(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn get_router() -> Router {
    router!(get "/" => view_home,
            get "/:index/_count" => view_count,
            get "/:index/_search" => view_search,
            post "/:index/_count" => view_count,
            post "/:index/_search" => view_search,
            get "/:index/:mapping/:doc" => view_get_doc,
            put "/:index/:mapping/:doc" => view_put_doc,
            put "/:index" => view_put_index,
            put "/:index/_mapping/:mapping" => view_put_mapping,
            post "/_bulk" => view_post_bulk,
            post "/:index/_refresh" => view_post_refresh)
}
