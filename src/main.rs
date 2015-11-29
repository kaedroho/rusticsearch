extern crate iron;
extern crate router;

use std::io::Read;

use iron::prelude::*;
use iron::status;
use router::Router;


fn main() {
    let mut router = Router::new();

    fn index(_: &mut Request) -> IronResult<Response> {
        Ok(Response::with((status::Ok, "Hello World!")))
    }

    router.get("/", index);

    fn count(req: &mut Request) -> IronResult<Response> {
        let ref index = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

        let mut payload = String::new();
        req.body.read_to_string(&mut payload).unwrap();

        // TODO

        let mut response = Response::with((status::Ok, "{\"count\": 0}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    }

    router.get("/:index/_count", count);

    fn search(req: &mut Request) -> IronResult<Response> {
        let ref index = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

        let mut payload = String::new();
        req.body.read_to_string(&mut payload).unwrap();

        // TODO

        let mut response = Response::with((status::Ok, "{\"hits\": {\"total\": 0, \"hits\": []}}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    }

    router.get("/:index/_search", search);

    fn put(req: &mut Request) -> IronResult<Response> {
        let ref index = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");
        let ref mapping = req.extensions.get::<Router>().unwrap().find("mapping").unwrap_or("");
        let ref doc = req.extensions.get::<Router>().unwrap().find("doc").unwrap_or("");

        let mut payload = String::new();
        req.body.read_to_string(&mut payload).unwrap();

        // TODO

        let mut response = Response::with((status::Ok, "{}"));
        response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
        Ok(response)
    }

    router.put("/:index/:mapping/:doc", put);

    Iron::new(router).http("localhost:9200").unwrap();
}
