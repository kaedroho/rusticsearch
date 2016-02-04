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


pub fn view_count(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    // Load query from body
    let mut payload = String::new();
    req.body.read_to_string(&mut payload).unwrap();

    let count = if !payload.is_empty() {
        let query_data = match Json::from_str(&payload) {
            Ok(data) => data,
            Err(error) => {
                // TODO: What specifically is bad about the JSON?
                let mut response = Response::with((status::BadRequest,
                                                   "{\"message\": \"Couldn't parse JSON\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        };

        // Parse query
        let query = query::parse_query(query_data.as_object().unwrap().get("query").unwrap());
        debug!("{:#?}", query);

        match query {
            Ok(query) => {
                let mut count = 0;
                for (_, doc) in index.docs.iter() {
                    if query.matches(&doc) {
                        count += 1;
                    }
                }

                count
            }
            Err(error) => {
                // TODO: What specifically is bad about the Query?
                let mut response = Response::with((status::BadRequest,
                                                   "{\"message\": \"Query error\"}"));
                response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                return Ok(response);
            }
        }

    } else {
        index.docs.len()
    };

    return json_response!(status::Ok, format!("{{\"count\": {}}}", count));
}


pub fn view_search(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    let data = json_from_request_body!(req);
    debug!("{:#?}", query::parse_query(data.unwrap().as_object().unwrap().get("query").unwrap()));

    // TODO: Run query

    return json_response!(status::Ok, "{\"hits\": {\"total\": 0, \"hits\": []}}");
}
