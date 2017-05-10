#[macro_use] mod utils;
mod index;
mod alias;
mod mapping;
mod document;
mod bulk;
mod search;

use std::sync::Arc;

use serde_json::Value;

use rocket;
use rocket::config::{ConfigBuilder, Environment};
use rocket_contrib::JSON;

use VERSION;
use system::System;


#[get("/")]
fn root() -> JSON<Value> {
    JSON(json!({
        "cluster_name": "rusticsearch",
        "version": {
            "number": VERSION
        }
    }))
}


pub fn api_main(system: Arc<System>) {
    let config = ConfigBuilder::new(Environment::Development)
        .address("127.0.0.1")
        .port(9200)
        .workers(4)
        .finalize()
        .unwrap();
    rocket::custom(config, true)
        .mount("/", routes![
            root,
            index::get,
            index::put,
            index::delete,
            index::refresh,
            alias::get_global,
            alias::get_list,
            alias::get,
            alias::put,
            mapping::put,
            document::get,
            document::put,
            document::delete,
            bulk::bulk,
            search::count,
            search::search,
        ])
        .manage(system)
        .launch();
}
