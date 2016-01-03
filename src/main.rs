extern crate iron;
#[macro_use]
extern crate router;
extern crate persistent;
extern crate rustc_serialize;

mod views;
mod query;
mod mapping;

use std::sync::Mutex;
use std::collections::HashMap;

use iron::prelude::*;
use iron::typemap::Key;
use rustc_serialize::json::Json;


#[derive(Debug)]
struct Document {
    data: Json,
}

impl Document {
    fn from_json(data: Json) -> Document {
        Document { data: data }
    }
}


#[derive(Debug)]
struct Mapping {
    pub docs: HashMap<String, Document>,
    pub fields: HashMap<String, mapping::FieldMapping>,
}

impl Mapping {
    fn from_json(json: &Json) -> Mapping {
        let json = json.as_object().unwrap();
        let properties_json = json.get("properties").unwrap().as_object().unwrap();

        // Parse fields
        let mut fields = HashMap::new();
        for (field_name, field_mapping_json) in properties_json.iter() {
            fields.insert(field_name.clone(), mapping::FieldMapping::from_json(field_mapping_json));
        }

        Mapping {
            docs: HashMap::new(),
            fields: fields,
        }
    }
}


#[derive(Debug)]
struct Index {
    pub mappings: HashMap<String, Mapping>,
}


impl Index {
    fn new() -> Index {
        Index { mappings: HashMap::new() }
    }
}


struct Globals {
    pub indices: Mutex<HashMap<String, Index>>,
}


impl Globals {
    fn new() -> Globals {
        Globals { indices: Mutex::new(HashMap::new()) }
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
