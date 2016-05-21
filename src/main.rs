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
mod term;
mod index;
mod query;
mod mapping;
mod analysis;
mod logger;

use std::sync::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::fs;

use iron::prelude::*;
use iron::typemap::Key;
use rustc_serialize::json::Json;

use index::Index;


const VERSION: &'static str = "0.1a0";


#[derive(Debug)]
pub struct Document {
    id: String,
    fields: BTreeMap<String, Vec<term::Term>>,
}

impl Document {
    pub fn from_json(id: String, data: Json, mapping: &mapping::Mapping) -> Document {
        let mut fields = BTreeMap::new();
        let mut all_field_tokens: Vec<term::Term> = Vec::new();

        for (field_name, field_value) in data.as_object().unwrap() {
            let processed_value = if let Some(field_mapping) = mapping.fields.get(field_name) {
                let value = field_mapping.process_value(field_value.clone());

                match value {
                    Some(ref value) => {
                        if field_mapping.is_in_all {
                            all_field_tokens.extend(value.iter().cloned());
                        }
                    }
                    None => {
                        warn!("Unprocessable value: {}", field_value);
                    }
                }

                value
            } else {
                Some(vec![term::Term::from_json(field_value)])
            };

            if let Some(field_value) = processed_value {
                fields.insert(field_name.clone(), field_value);
            }
        }

        // Insert _all field
        fields.insert("_all".to_owned(), all_field_tokens);

        Document {
            id: id,
            fields: fields,
        }
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

    println!("{:?}",
             analysis::Analyzer::EdgeNGram.run("Up from the bowels of hell he sail. Weilding a \
                                                tankard of freshly brewed ale."
                                                   .to_string()));

    logger::init().unwrap();

    let indices_path = Path::new("./indices").to_path_buf();
    let indices = load_indices(&indices_path.as_path());
    let router = api::get_router();
    let mut chain = Chain::new(router);
    chain.link(persistent::Read::<Globals>::both(Globals::new(indices_path, indices)));
    Iron::new(chain).http("localhost:9200").unwrap();
}
