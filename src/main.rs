extern crate iron;
#[macro_use]
extern crate router;
extern crate persistent;
extern crate url;
extern crate rustc_serialize;
extern crate unidecode;
extern crate unicode_segmentation;
#[macro_use]
extern crate log;

mod api;
mod token;
mod index;
mod mapping;
mod analysis;
mod search;
mod logger;

use std::sync::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;

use iron::prelude::*;
use iron::typemap::Key;

use index::Index;


const VERSION: &'static str = "0.1a0";


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
    Index::new()
}


fn load_indices(indices_path: &Path) -> HashMap<String, Index> {
    let mut indices = HashMap::new();

    for file in fs::read_dir(&indices_path).unwrap() {
        let path = file.unwrap().path();
        let index_name: String = path.file_stem().unwrap().to_str().unwrap().to_owned();

        if let Some(ext) = path.extension() {
            if ext.to_str() == Some("rsi") {
                info!("Loaded index: {}", index_name);
                indices.insert(index_name, load_index(path.as_path()));
            }
        }
    }

    indices
}


fn main() {
    println!("rusticsearch ({})", VERSION);
    println!("");

    logger::init().unwrap();

    let indices_path = Path::new("./indices").to_path_buf();
    let indices = load_indices(&indices_path.as_path());
    let router = api::get_router();
    let mut chain = Chain::new(router);
    chain.link(persistent::Read::<Globals>::both(Globals::new(indices_path, indices)));
    Iron::new(chain).http("localhost:9200").unwrap();
}
