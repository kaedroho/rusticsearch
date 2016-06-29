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

mod system;
mod api;
mod search;
mod logger;

use std::sync::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;

use iron::prelude::*;
use iron::typemap::Key;

use system::System;
use search::index::Index;


const VERSION: &'static str = "0.1a0";


impl Key for System {
    type Value = System;
}


fn main() {
    println!("rusticsearch ({})", VERSION);
    println!("");

    logger::init().unwrap();

    let mut system = System::new(Path::new("data/").to_path_buf());
    system.load_indices();

    let router = api::get_router();
    let mut chain = Chain::new(router);
    chain.link(persistent::Read::<System>::both(system));
    Iron::new(chain).http("localhost:9200").unwrap();
}
