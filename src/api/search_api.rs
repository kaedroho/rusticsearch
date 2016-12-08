use std::io::Read;
use std::collections::BTreeMap;

use rustc_serialize::json::{self, Json};
use url::form_urlencoded;
use kite::query::Query;
use kite::collectors::top_score::TopScoreCollector;
use kite::collectors::total_count::TotalCountCollector;
use kite_rocksdb::DocRef;

use query_parser::{QueryParseContext, parse as parse_query};

use api::persistent;
use api::iron::prelude::*;
use api::iron::status;
use api::router::Router;
use api::utils::json_response;


pub fn view_count(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);
    let index_reader = index.store.reader();

    let count = match json_from_request_body!(req) {
        Some(query_json) => {
            // Parse query
            let query = parse_query(&QueryParseContext::new().set_mappings(&index.mappings).no_score(), query_json.as_object().unwrap().get("query").unwrap());
            debug!("{:#?}", query);

            match query {
                Ok(query) => {
                    let mut collector = TotalCountCollector::new();
                    index_reader.search(&mut collector, &query.build()).unwrap();
                    collector.get_total_count()
                }
                Err(_) => {
                    // TODO: What specifically is bad about the Query?
                    let mut response = Response::with((status::BadRequest,
                                                       "{\"message\": \"Query error\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    return Ok(response);
                }
            }
        }
        None => {
            let mut collector = TotalCountCollector::new();
            index_reader.search(&mut collector, &Query::new_match_all()).unwrap();
            collector.get_total_count()
        }
    };

    return Ok(json_response(status::Ok, format!("{{\"count\": {}}}", count)));
}


pub fn view_search(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);
    let index_reader = index.store.reader();

    match json_from_request_body!(req) {
        Some(query_json) => {
            // Parse query
            let query = parse_query(&QueryParseContext::new().set_mappings(&index.mappings), query_json.as_object().unwrap().get("query").unwrap());
            debug!("{:#?}", query);

            match query {
                Ok(query) => {
                    let mut from = 0;
                    let mut size = 10;
                    let mut fields = Vec::new();

                    // TODO: Rewrite this
                    if let Some(ref url_query) = req.url.query() {
                        for (key, value) in form_urlencoded::parse(url_query.as_bytes()) {
                            match key.as_ref() {
                                "from" => {
                                    from = value.as_ref().parse().expect("need a number");
                                }
                                "size" => {
                                    size = value.as_ref().parse().expect("need a number");
                                }
                                "fields" => {
                                    for field_name in value.split(",") {
                                        let field_ref = match index_reader.schema().get_field_by_name(field_name) {
                                            Some(field_ref) => field_ref,
                                            None => {
                                                warn!("unknown field {:?}", field_name);
                                                continue;
                                            }
                                        };

                                        fields.push((field_name.to_owned(), field_ref));
                                    }
                                }
                                // terminate_after
                                // explain
                                // version
                                // timeout
                                // fielddata_fields
                                // track_scores
                                // stats
                                // suggest_field
                                _ => warn!("unrecognised GET parameter {:?}", key),
                            }
                        }
                    }

                    // Do the search
                    let mut collector = TopScoreCollector::new(from + size);
                    index_reader.search(&mut collector, &query.build()).unwrap();

                    // TODO: {"took":5,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":4,"max_score":1.0,"hits":[{"_index":"wagtail","_type":"searchtests_searchtest_searchtests_searchtestchild","_id":"searchtests_searchtest:5380","_score":1.0,"fields":{"pk":["5380"]}},{"_index":"wagtail","_type":"searchtests_searchtest","_id":"searchtests_searchtest:5379","_score":1.0,"fields":{"pk":["5379"]}}]}}
                    Ok(json_response(status::Ok,
                                     format!("{{\"hits\": {{\"total\": {}, \"hits\": {}}}}}",
                                             0, // TODO
                                             json::encode(&Json::Array(collector.into_sorted_vec().iter()
                                                .skip(from)
                                                .map(|doc_match| {
                                                    let mut field_values = BTreeMap::new();

                                                    for &(ref field_name, field_ref) in fields.iter() {
                                                        let value = match index_reader.read_stored_field(field_ref, DocRef::from_u64(doc_match.doc_id())) {
                                                            Ok(Some(value)) => {
                                                                value.as_json()
                                                            }
                                                            Ok(None) => Json::Array(vec![]),
                                                            Err(_) => Json::Array(vec![]),
                                                        };

                                                        field_values.insert(field_name.clone(), Json::Array(vec![value]));
                                                    }

                                                    let mut hit = BTreeMap::new();
                                                    hit.insert("_score".to_owned(), Json::F64(doc_match.score().unwrap()));
                                                    hit.insert("fields".to_owned(), Json::Object(field_values));
                                                    Json::Object(hit)
                                                })
                                                .collect())
                                            ).unwrap())))
                }
                Err(_) => {
                    // TODO: What specifically is bad about the Query?
                    let mut response = Response::with((status::BadRequest,
                                                       "{\"message\": \"Query error\"}"));
                    response.headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                    Ok(response)
                }
            }
        }
        None => Ok(json_response(status::BadRequest, "{\"message\": \"Missing query\"}")),
    }
}
