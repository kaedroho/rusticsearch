extern crate iron;
extern crate router;

use iron::prelude::*;
use iron::status;
use router::Router;


fn main() {
    let mut router = Router::new();

    fn index(_: &mut Request) -> IronResult<Response> {
        Ok(Response::with((status::Ok, "Hello World!")))
    }

    router.get("/", index);

    Iron::new(router).http("localhost:9200").unwrap();
}
