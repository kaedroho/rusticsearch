extern crate iron;
extern crate router;

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use iron::prelude::*;
use iron::status;
use router::Router;

#[derive(Debug)]
struct Index;


impl Index {
    fn new() -> Index {
        Index
    }
}


fn main() {
    let mut indices = Arc::new(Mutex::new(HashMap::new()));
    indices.lock().unwrap().insert("wagtail", Index::new());

    let mut router = Router::new();

    router.get("/", |_: &mut Request| -> IronResult<Response> {
        Ok(Response::with((status::Ok, "Hello World!")))
    });

    {
        let indices = indices.clone();

        router.get("/:index/_count", move |req: &mut Request| -> IronResult<Response> {
            let ref index = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

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
            let ref mapping = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");
            let ref doc = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

            let mut payload = String::new();
            req.body.read_to_string(&mut payload).unwrap();

            // TODO

            let mut response = Response::with((status::Ok, "{}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            Ok(response)
        });
    }

    Iron::new(router).http("localhost:9200").unwrap();
}
