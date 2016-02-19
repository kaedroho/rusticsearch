extern crate router;
extern crate persistent;

#[macro_use]
mod macros;
mod search_api;
mod alias_api;
mod document_api;
mod index_api;
mod mapping_api;
mod bulk_api;

use iron::prelude::*;
use iron::status;
use router::Router;


fn index_not_found_response() -> Response {
    let mut response = Response::with((status::NotFound, "{\"message\": \"Index not found\"}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    return response;
}


pub fn view_home(_: &mut Request) -> IronResult<Response> {
    Ok(Response::with((status::Ok, "Hello World!")))
}


pub fn get_router() -> Router {
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
