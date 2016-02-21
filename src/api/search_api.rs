use std::io::Read;
use std::collections::{BTreeMap, BinaryHeap};
use std::cmp::Ordering;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::{self, Json};

use query::Query;
use query::parse::parse_query;
use super::persistent;
use super::utils::json_response;
use super::super::{Globals, Document};


#[derive(Debug)]
struct SearchHit<'a> {
    doc: &'a Document,
    score: f64,
}


impl<'a> SearchHit<'a> {
    fn as_json(&self) -> Json {
        let mut pk_field: Vec<Json> = Vec::new();
        pk_field.push(Json::String(self.doc.data.as_object().unwrap().get("pk").unwrap().as_string().unwrap().to_owned()));

        let mut fields = BTreeMap::new();
        fields.insert("pk".to_owned(), Json::Array(pk_field));

        let mut hit = BTreeMap::new();
        hit.insert("_score".to_owned(), Json::F64(self.score));
        hit.insert("fields".to_owned(), Json::Object(fields));
        Json::Object(hit)
    }
}


pub fn view_count(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    let count = match json_from_request_body!(req) {
        Some(query_json) => {
            // Parse query
            let query = parse_query(query_json.as_object().unwrap().get("query").unwrap());
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
        }
        None => index.docs.len()
    };

    return Ok(json_response(status::Ok, format!("{{\"count\": {}}}", count)));
}


pub fn view_search(req: &mut Request) -> IronResult<Response> {
    let ref glob = get_globals!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let indices = glob.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);

    match json_from_request_body!(req) {
        Some(query_json) => {
            // Parse query
            let query = parse_query(query_json.as_object().unwrap().get("query").unwrap());
            debug!("{:#?}", query);

            match query {
                Ok(query) => {
                    let mut total = 0;
                    let mut hits = Vec::new();
                    for (_, doc) in index.docs.iter() {
                        if let Some(score) = query.rank(&doc) {
                            let hit = SearchHit{
                                doc: &doc,
                                score: score,
                            };

                            hits.push(hit.as_json());
                            total += 1;
                        }
                    }

                    // TODO: {"took":5,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":4,"max_score":1.0,"hits":[{"_index":"wagtail","_type":"searchtests_searchtest_searchtests_searchtestchild","_id":"searchtests_searchtest:5380","_score":1.0,"fields":{"pk":["5380"]}},{"_index":"wagtail","_type":"searchtests_searchtest","_id":"searchtests_searchtest:5379","_score":1.0,"fields":{"pk":["5379"]}}]}}
                    Ok(json_response(status::Ok, format!("{{\"hits\": {{\"total\": {}, \"hits\": {}}}}}", total, json::encode(&Json::Array(hits)).unwrap())))
                }
                Err(error) => {
                    // TODO: What specifically is bad about the Query?
                    let mut response = Response::with((status::BadRequest,
                                                       "{\"message\": \"Query error\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    Ok(response)
                }
            }
        }
        None => {
            Ok(json_response(status::BadRequest, "{\"message\": \"Missing query\"}"))
        }
    }
}
