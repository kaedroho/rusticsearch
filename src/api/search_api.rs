use std::io::Read;
use std::collections::{BTreeMap, BinaryHeap};
use std::cmp::Ordering;

use iron::prelude::*;
use iron::status;
use router::Router;
use rustc_serialize::json::{self, Json};
use url::form_urlencoded;

use query::Query;
use query::parse::parse_query;
use super::persistent;
use super::utils::json_response;
use super::super::{Globals, Document, Index};


#[derive(Debug)]
struct SearchHit<'a> {
    doc: &'a Document,
    score: f64,
}


impl<'a> SearchHit<'a> {
    fn as_json(&self) -> Json {
        let mut pk_field: Vec<Json> = Vec::new();
        pk_field.push(self.doc.fields.get("pk").unwrap().as_json());

        let mut fields = BTreeMap::new();
        fields.insert("pk".to_owned(), Json::Array(pk_field));

        let mut hit = BTreeMap::new();
        hit.insert("_score".to_owned(), Json::F64(self.score));
        hit.insert("fields".to_owned(), Json::Object(fields));
        Json::Object(hit)
    }
}


#[derive(Debug)]
struct SearchResponse<'a> {
    total_hits: usize,
    hits: Vec<SearchHit<'a>>,
}


#[derive(Debug)]
struct SearchRequest {
    query: Query,
    offset: usize,
    limit: usize,
}

impl SearchRequest {
    fn run<'a>(&self, index: &'a Index) -> SearchResponse<'a> {
        // Find all hits
        let mut hits = Vec::new();
        for (_, doc) in index.docs.iter() {
            if let Some(score) = self.query.rank(&doc) {
                hits.push(SearchHit {
                    doc: &doc,
                    score: score,
                });
            }
        }

        // Sort by score
        hits.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Less));

        // Pagination
        let total_hits = hits.len();
        if self.offset > 0 {
            hits.drain(..self.offset);
        }
        hits.truncate(self.limit);

        SearchResponse {
            total_hits: total_hits,
            hits: hits,
        }
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
                    let request = SearchRequest {
                        query: query,
                        offset: 0,
                        limit: 0,
                    };

                    request.run(&index).total_hits
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
                    let mut request = SearchRequest {
                        query: query,
                        offset: 0,
                        limit: 10,
                    };

                    // TODO: Rewrite this
                    if let Some(ref url_query) = req.url.query {
                        for (key, value) in form_urlencoded::parse(url_query.as_bytes()) {
                            match key.as_ref() {
                                "from" => {
                                    request.offset = value.as_ref().parse().expect("need a number");
                                }
                                "size" => {
                                    request.limit = value.as_ref().parse().expect("need a number");
                                }
                                // explain
                                // version
                                // timeout
                                // terminate_after
                                // fields
                                // fielddata_fields
                                // track_scores
                                // stats
                                // suggest_field
                                _ => warn!("unrecognised GET parameter {:?}", key),
                            }
                        }
                    }

                    let response = request.run(&index);

                    // TODO: {"took":5,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":4,"max_score":1.0,"hits":[{"_index":"wagtail","_type":"searchtests_searchtest_searchtests_searchtestchild","_id":"searchtests_searchtest:5380","_score":1.0,"fields":{"pk":["5380"]}},{"_index":"wagtail","_type":"searchtests_searchtest","_id":"searchtests_searchtest:5379","_score":1.0,"fields":{"pk":["5379"]}}]}}
                    Ok(json_response(status::Ok, format!("{{\"hits\": {{\"total\": {}, \"hits\": {}}}}}", response.total_hits, json::encode(&Json::Array(response.hits.iter().map(|hit| hit.as_json()).collect())).unwrap())))
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
