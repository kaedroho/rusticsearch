mod test_index_api;

use std::path::Path;
use std::sync::Arc;
use std::net::{SocketAddr, ToSocketAddrs};

use serde_json;
use slog_term;
use slog::Logger;

use logger;
use system::System;
use api::{get_router, Context};
use api::iron::{IronResult, Request, Response, Url, Headers};
use api::iron::middleware::Chain;
use api::iron::method::Method;
use api::iron::typemap::TypeMap;
use api::persistent;



fn request(method: Method, path: &str, data: Option<&serde_json::Value>) -> IronResult<Response> {
    // Start a dummy instance
    let log = Logger::new_root(o!());
    log.set_drain(slog_term::async_stderr());
    logger::init().unwrap();
    let system = Arc::new(System::new(log, Path::new("data/").to_path_buf()));

    // Get router
    let router = get_router();
    let mut chain = Chain::new(router);
    chain.link(persistent::Read::<Context>::both(Context::new(system.clone())));

    // Build request
    let mut request = Request {
        url: Url::parse("http://www.rust-lang.org").unwrap(),
        remote_addr: "localhost:3000".to_socket_addrs().unwrap().next().unwrap(),
        local_addr: "localhost:3000".to_socket_addrs().unwrap().next().unwrap(),
        headers: Headers::new(),
        body: unsafe { ::std::mem::uninitialized() }, // FIXME(reem): Ugh
        method: Method::Get,
        extensions: TypeMap::new(),
        version: HttpVersion::Http11,
        _p: (),
    };

    chain.handle(&mut request)
}



fn get(path: &str, data: Option<&serde_json::Value>) -> IronResult<Response> {
    request(Method::Get, path, data)
}


fn post(path: &str, data: Option<&serde_json::Value>) -> IronResult<Response> {
    request(Method::Post, path, data)
}


fn put(path: &str, data: Option<&serde_json::Value>) -> IronResult<Response> {
    request(Method::Put, path, data)
}


fn delete(path: &str, data: Option<&serde_json::Value>) -> IronResult<Response> {
    request(Method::Delete, path, data)
}
