extern crate router;
extern crate persistent;

use std::io::Read;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::{self, Json};

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
    let indices = glob.indices.lock().unwrap();

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

    println!("{:#?}", query::parse_query(data.unwrap().as_object().unwrap().get("query").unwrap()));

    // TODO: Run query

    // Temporary count and return numbers
    let mut count = 0;
    for mapping in index.mappings.values() {
        count += mapping.docs.len();
    }

    let mut response = Response::with((status::Ok, format!("{{\"count\": {}}}", count)));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_search(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.lock().unwrap();

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

    println!("{:#?}", query::parse_query(data.unwrap().as_object().unwrap().get("query").unwrap()));

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
    let indices = glob.indices.lock().unwrap();

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
    let doc = match mapping.docs.get(doc_id) {
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
    let mut indices = glob.indices.lock().unwrap();

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

    // Create and insert document
    if let Some(data) = data {
        let doc = Document::from_json(data);
        mapping.docs.insert(doc_id.clone().to_owned(), doc);
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
    let mut indices = glob.indices.lock().unwrap();

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
    indices.insert(index_name.clone().to_owned(), Index::new());

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
    let mut indices = glob.indices.lock().unwrap();

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
    println!("{:#?}", mapping);
    index.mappings.insert(mapping_name.clone().to_owned(), mapping);

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_post_bulk(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // Lock index array
    let mut indices = glob.indices.lock().unwrap();

    // Load data from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

    for payload_part in payload.split('\n') {
        let data = if !payload.is_empty() {
            Some(Json::from_str(&payload))
        } else {
            None
        };
    }

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_post_refresh(req: &mut Request) -> IronResult<Response> {
    let ref glob = req.get::<persistent::Read<Globals>>().unwrap();

    // Lock index array
    let mut indices = glob.indices.lock().unwrap();

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
            post ":index/_refresh" => view_post_refresh)
}
