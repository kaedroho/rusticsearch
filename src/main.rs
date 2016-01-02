extern crate iron;
#[macro_use]
extern crate router;
extern crate persistent;
extern crate rustc_serialize;

mod views;

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
enum Filter {
    Not(Box<Filter>),
    Or(Vec<Filter>),
    And(Vec<Filter>),
    Term(String, String),
}


impl Filter {
    fn matches(&self, doc: &Document) -> bool {
        match *self {
            Filter::Not(ref filter) => !filter.matches(doc),
            Filter::Or(ref filters) => {
                for filter in filters.iter() {
                    if (filter.matches(doc)) {
                        return true;
                    }
                }

                false
            },
            Filter::And(ref filters) => {
                for filter in filters.iter() {
                    if (!filter.matches(doc)) {
                        return false;
                    }
                }

                true
            },
            Filter::Term(ref field, ref value) => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    if let Json::String(ref field_value) = *field_value {
                        return field_value == value
                    }
                }

                false
            }
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

impl Key for Globals { type Value = Globals; }



fn main() {
    let f = Filter::Or(vec![
        Filter::Term("title".to_owned(), "test".to_owned()),
        Filter::Term("title".to_owned(), "foo".to_owned()),
    ]);

    println!("{:?}", f);

    let router = views::get_router();
    let mut chain = Chain::new(router);
    chain.link(persistent::Read::<Globals>::both(Globals::new()));
    Iron::new(chain).http("localhost:9200").unwrap();
}
