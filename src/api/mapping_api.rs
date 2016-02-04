use std::io::Read;
use std::collections::HashMap;
use std::fs;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::{self, Json};
use rusqlite::Connection;

use super::{persistent, index_not_found_response};
use super::super::{Globals, Index, mapping, Document, query};


pub fn view_put_mapping(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");

    // Lock index array
    let mut indices = glob.indices.write().unwrap();

    // Get index
    let mut index = get_index_or_404_mut!(indices, *index_name);

    // Load data from body
    let data = json_from_request_body!(req);

    let data = match data {
        Some(data) => data,
        None => {
            // TODO: Better error
            let mut response = Response::with((status::Ok, "{\"acknowledged\": false}"));
            response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
            return Ok(response)
        },
    };

    let data = data.as_object().unwrap().get(*mapping_name).unwrap();

    // Insert mapping
    let mapping = mapping::Mapping::from_json(&data);
    debug!("{:#?}", mapping);
    index.mappings.insert(mapping_name.clone().to_owned(), mapping);

    let mut response = Response::with((status::Ok, "{\"acknowledged\": true}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}
