extern crate iron;
#[macro_use]
extern crate router;
extern crate persistent;
extern crate rustc_serialize;
extern crate rusqlite;
#[macro_use]
extern crate log;

mod views;
mod query;
mod mapping;
mod logger;

use std::sync::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;

use iron::prelude::*;
use iron::typemap::Key;
use rustc_serialize::json::Json;
use rusqlite::Connection;


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
            fields: fields,
        }
    }
}


#[derive(Debug)]
struct Index {
    pub connection: Mutex<Connection>,
    pub mappings: HashMap<String, Mapping>,
    pub docs: HashMap<String, Document>,
}


impl Index {
    fn new(connection: Connection) -> Index {
        Index {
            connection: Mutex::new(connection),
            mappings: HashMap::new(),
            docs: HashMap::new(),
        }
    }

    fn initialise(&mut self) {
        let connection = self.connection.lock().unwrap();

        connection.execute("CREATE TABLE document (
              id              INTEGER PRIMARY KEY,
              mapping         TEXT NOT NULL,
              data            BLOB
              )", &[]).unwrap();
    }
}


struct Globals {
    pub indices_path: PathBuf,
    pub indices: RwLock<HashMap<String, Index>>,
}


impl Globals {
    fn new(indices_path: PathBuf, indices: HashMap<String, Index>) -> Globals {
        Globals {
            indices_path: indices_path,
            indices: RwLock::new(indices),
        }
    }
}


impl Key for Globals {
    type Value = Globals;
}


fn load_index(path: &Path) -> Index {
    Index::new(Connection::open(path).unwrap())
}


fn load_indices(indices_path: &Path) -> HashMap<String, Index> {
    let mut indices = HashMap::new();

    for file in fs::read_dir(&indices_path).unwrap() {
        let path = file.unwrap().path();
        let index_name: String = path.file_stem().unwrap().to_str().unwrap().to_owned();
        if path.extension().unwrap().to_str() == Some("rsi") {
            info!("Loaded index: {}", index_name);
            indices.insert(index_name, load_index(path.as_path()));
        }
    }

    indices
}


fn main() {
    logger::init().unwrap();

    let indices_path = Path::new("./indices").to_path_buf();
    let indices = load_indices(&indices_path.as_path());
    let router = views::get_router();
    let mut chain = Chain::new(router);
    chain.link(persistent::Read::<Globals>::both(Globals::new(indices_path, indices)));
    Iron::new(chain).http("localhost:9200").unwrap();
}
