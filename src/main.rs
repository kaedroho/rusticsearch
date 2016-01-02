extern crate iron;
#[macro_use]
extern crate router;
extern crate persistent;
extern crate rustc_serialize;

mod views;
mod query;

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use iron::prelude::*;
use iron::typemap::Key;
use iron::status;
use router::Router;
use rustc_serialize::json::{self, Json};


#[derive(Debug)]
struct Document {
    data: Json,
}

impl Document {
    fn from_json(data: Json) -> Document {
        Document{
            data: data,
        }
    }
}


#[derive(Debug)]
struct Mapping {
    pub docs: HashMap<String, Document>,
}

impl Mapping {
    fn new() -> Mapping {
        Mapping{
            docs: HashMap::new(),
        }
    }
}


#[derive(Debug)]
struct Index {
    pub mappings: HashMap<String, Mapping>,
}


impl Index {
    fn new() -> Index {
        Index{
            mappings: HashMap::new(),
        }
    }
}


struct Globals {
    pub indices: Mutex<HashMap<String, Index>>,
}


impl Globals {
    fn new() -> Globals {
        Globals {
            indices: Mutex::new(HashMap::new())
        }
    }
}


impl Key for Globals {
    type Value = Globals;
}


fn main() {
    let router = views::get_router();
    let mut chain = Chain::new(router);
    chain.link(persistent::Read::<Globals>::both(Globals::new()));
    Iron::new(chain).http("localhost:9200").unwrap();
}
