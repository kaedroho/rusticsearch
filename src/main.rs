#![feature(btree_range, collections_bound)]

#[macro_use]
extern crate router;
extern crate url;
extern crate rustc_serialize;
extern crate unidecode;
extern crate unicode_segmentation;
#[macro_use]
extern crate log;
#[macro_use]
extern crate maplit;

mod system;
mod api;
mod search;
mod logger;

use std::path::Path;
use std::sync::Arc;

use system::System;


const VERSION: &'static str = "0.1a0";


fn main() {
    println!("rusticsearch ({})", VERSION);
    println!("");

    logger::init().unwrap();

    let system = Arc::new(System::new(Path::new("data/").to_path_buf()));

    println!("Loading indices");
    system.load_indices();

    println!("Starting API");
    api::api_main(system);
}
