extern crate iron;
extern crate router;
extern crate rustc_serialize;

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::Json;


#[derive(Debug)]
struct Mapping;

impl Mapping {
    fn new() -> Mapping {
        Mapping
    }
}


#[derive(Debug)]
struct Index {
    pub mappings: HashMap<&'static str, Mapping>,
}


impl Index {
    fn new() -> Index {
        Index{
            mappings: HashMap::new(),
        }
    }
}


fn index_not_found_response() -> Response {
    let mut response = Response::with((status::NotFound, "{\"message\": \"Index not found\"}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    return response;
}


fn main() {
    let mut indices = Arc::new(Mutex::new(HashMap::new()));
    let mut wagtail_index = Index::new();
    wagtail_index.mappings.insert("wagtaildocs_document", Mapping::new());
    indices.lock().unwrap().insert("wagtail", wagtail_index);

    let mut router = Router::new();

    router.get("/", |_: &mut Request| -> IronResult<Response> {
        Ok(Response::with((status::Ok, "Hello World!")))
    });

    {
        let indices = indices.clone();

        router.get("/:index/_count", move |req: &mut Request| -> IronResult<Response> {
            // URL parameters
            let ref index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

            // Lock index array
            let mut indices = indices.lock().unwrap();

            // Find index
            let mut index = match indices.get(index_name) {
                Some(index) => index,
                None => {
                    return Ok(index_not_found_response());
                }
            };

            // Load query from body
            let mut payload = String::new();
            req.body.read_to_string(&mut payload).unwrap();

            // TODO: Run query

            let mut response = Response::with((status::Ok, "{\"count\": 0}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            Ok(response)
        });
    }

    {
        let indices = indices.clone();

        router.get("/:index/_search", move |req: &mut Request| -> IronResult<Response> {
            // URL parameters
            let ref index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

            // Lock index array
            let mut indices = indices.lock().unwrap();

            // Find index
            let mut index = match indices.get(index_name) {
                Some(index) => index,
                None => {
                    return Ok(index_not_found_response());
                }
            };

            // Load query from body
            let mut payload = String::new();
            req.body.read_to_string(&mut payload).unwrap();

            // TODO: Run query

            let mut response = Response::with((status::Ok, "{\"hits\": {\"total\": 0, \"hits\": []}}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            Ok(response)
        });
    }

    {
        let indices = indices.clone();

        router.put("/:index/:mapping/:doc", move |req: &mut Request| -> IronResult<Response> {
            // URL parameters
            let ref index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
            let ref mapping_name = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");
            let ref doc_id = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

            // Lock index array
            let mut indices = indices.lock().unwrap();

            // Find index
            let mut index = match indices.get(index_name) {
                Some(index) => index,
                None => {
                    return Ok(index_not_found_response());
                }
            };

            // Find mapping
            let mut mapping = match index.mappings.get(mapping_name) {
                Some(mapping) => mapping,
                None => {
                    let mut response = Response::with((status::NotFound, "{\"message\": \"Mapping not found\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    return Ok(response);
                }
            };

            // Load data from body
            let mut payload = String::new();
            req.body.read_to_string(&mut payload).unwrap();

            let data = match Json::from_str(&payload) {
                Ok(data) => data,
                Err(error) => {
                    // TODO: What specifically is bad about the JSON?
                    let mut response = Response::with((status::BadRequest, "{\"message\": \"Couldn't parse JSON\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    return Ok(response);
                }
            };

            // TODO: Validate and insert document

            let mut response = Response::with((status::Ok, "{}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            Ok(response)
        });
    }

    Iron::new(router).http("localhost:9200").unwrap();
}
