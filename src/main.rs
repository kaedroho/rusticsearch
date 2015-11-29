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
            let ref index = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
            if !indices.lock().unwrap().contains_key(index) {
                return Ok(index_not_found_response());
            }

            let mut payload = String::new();
            req.body.read_to_string(&mut payload).unwrap();

            // TODO

            let mut response = Response::with((status::Ok, "{\"count\": 0}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            Ok(response)
        });
    }

    {
        let indices = indices.clone();

        router.get("/:index/_search", move |req: &mut Request| -> IronResult<Response> {
            let ref index = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
            if !indices.lock().unwrap().contains_key(index) {
                return Ok(index_not_found_response());
            }

            let mut payload = String::new();
            req.body.read_to_string(&mut payload).unwrap();

            // TODO

            let mut response = Response::with((status::Ok, "{\"hits\": {\"total\": 0, \"hits\": []}}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            Ok(response)
        });
    }

    {
        let indices = indices.clone();

        router.put("/:index/:mapping/:doc", move |req: &mut Request| -> IronResult<Response> {
            let ref index = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
            if !indices.lock().unwrap().contains_key(index) {
                return Ok(index_not_found_response());
            }

            let ref mapping = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");
            if !indices.lock().unwrap().get(index).unwrap().mappings.contains_key(mapping) {
                let mut response = Response::with((status::NotFound, "{\"message\": \"Mapping not found\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }

            let ref doc = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

            let mut payload = String::new();
            req.body.read_to_string(&mut payload).unwrap();

            let data = Json::from_str(&payload).unwrap();

            // TODO

            let mut response = Response::with((status::Ok, "{}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            Ok(response)
        });
    }

    Iron::new(router).http("localhost:9200").unwrap();
}
