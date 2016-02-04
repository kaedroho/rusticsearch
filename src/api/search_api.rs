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

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Find index
    let index = match indices.get(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

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

    let mut response = Response::with((status::Ok, format!("{{\"count\": {}}}", count)));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}


pub fn view_search(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);

    // URL parameters
    let index_name = req.extensions.get::<Router>().unwrap().find("index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Find index
    let index = match indices.get(index_name) {
        Some(index) => index,
        None => {
            return Ok(index_not_found_response());
        }
    };

    let data = json_from_request_body!(req);
    debug!("{:#?}", query::parse_query(data.unwrap().as_object().unwrap().get("query").unwrap()));

    // TODO: Run query

    let mut response = Response::with((status::Ok, "{\"hits\": {\"total\": 0, \"hits\": []}}"));
    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
    Ok(response)
}
