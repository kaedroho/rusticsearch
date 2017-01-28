extern crate iron;
extern crate router;
extern crate persistent;

#[macro_use]
mod utils;
mod search_api;
mod alias_api;
mod document_api;
mod index_api;
mod mapping_api;
mod bulk_api;

use std::sync::Arc;

use api::iron::prelude::*;
use api::iron::status;
use api::iron::typemap::Key;
use api::router::Router;
use api::utils::json_response;

use system::System;
use VERSION;


fn view_home(_: &mut Request) -> IronResult<Response> {
    Ok(json_response(status::Ok, json!({
        "cluster_name": "rusticsearch",
        "version": {
            "number": VERSION
        }
    })))
}


fn get_router() -> Router {
    router!(get "/" => view_home,
            get "/:index/_count" => search_api::view_count,
            post "/:index/_count" => search_api::view_count,
            get "/:index/_search" => search_api::view_search,
            post "/:index/_search" => search_api::view_search,
            get "/_alias/:alias" => alias_api::view_get_global_alias,
            get "/:index/_alias" => alias_api::view_get_alias_list,
            get "/:index/_alias/:alias" => alias_api::view_get_alias,
            put "/:index/_alias/:alias" => alias_api::view_put_alias,
            get "/:index/:mapping/:doc" => document_api::view_get_doc,
            put "/:index/:mapping/:doc" => document_api::view_put_doc,
            delete "/:index/:mapping/:doc" => document_api::view_delete_doc,
            get "/:index" => index_api::view_get_index,
            put "/:index" => index_api::view_put_index,
            delete "/:index" => index_api::view_delete_index,
            post "/:index/_refresh" => index_api::view_post_refresh_index,
            put "/:index/_mapping/:mapping" => mapping_api::view_put_mapping,
            post "/_bulk" => bulk_api::view_post_bulk)
}


// The "Context" struct just wraps Arc<System> so we can put it into chain.link()
// Workaround for: https://github.com/iron/persistent/issues/55

struct Context {
    system: Arc<System>,
}


impl Context {
    fn new(system: Arc<System>) -> Context {
        Context {
            system: system,
        }
    }
}


impl Key for Context {
    type Value = Context;
}


pub fn api_main(system: Arc<System>) {
    let router = get_router();
    let mut chain = Chain::new(router);
    chain.link(persistent::Read::<Context>::both(Context::new(system.clone())));
    system.log.info("[api] listening", b!("scheme" => "http", "address" => "localhost", "port" => 9200));

    if let Err(error) = Iron::new(chain).http("localhost:9200") {
        system.log.critical("[api] unable to start api server", b!("error" => format!("{}", error)));
    }
}
