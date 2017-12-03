use std::sync::Arc;

use futures::{self, Stream};
use futures::future::Future;
use tokio_core::reactor::Core;
use tokio_core::net::TcpListener;
use hyper::{self, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Http, Request, Response, Service};

use system::System;

struct Server {
    system: Arc<System>,
}

impl Service for Server {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let message = "Hi";
        return Box::new(futures::future::ok(
            Response::new()
                .with_status(StatusCode::Ok)
                .with_header(ContentLength(message.len() as u64))
                .with_body(message)
        ));
    }
}

pub fn main(system: Arc<System>) {
    info!(system.log, "listening"; "scheme" => "http", "address" => "127.0.0.1:9200");

    let new_system = system.clone();

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let listener = match TcpListener::bind(&"127.0.0.1:9200".parse().unwrap(), &handle) {
        Ok(listener) => listener,
        Err(error) => {
            crit!(system.log, "unable to start server"; "error" => format!("{}", error));
            return;
        }
    };

    let server = listener.incoming().for_each(move |(sock, addr)| {
        let s = Server {
            system: new_system.clone(),
        };
        Http::new().bind_connection(&handle, sock, addr, s);

        Ok(())
    });
    core.run(server).unwrap();
}
