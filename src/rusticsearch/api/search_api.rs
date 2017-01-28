use std::io::Read;
use std::collections::BTreeMap;

use serde_json;
use url::form_urlencoded;
use kite::document::DocRef;
use kite::query::Query;
use kite::collectors::top_score::TopScoreCollector;
use kite::collectors::total_count::TotalCountCollector;

use query_parser::{QueryBuildContext, parse as parse_query};

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
    let index_metadata = index.metadata.read().unwrap();

    let count = match json_from_request_body!(req) {
        Some(query_json) => {
            // Parse query
            let query = parse_query(query_json.as_object().unwrap().get("query").unwrap());
            debug!("{:#?}", query);

            match query {
                Ok(query) => {
                    let mut collector = TotalCountCollector::new();
                    index_reader.search(&mut collector, &query.build(&QueryBuildContext::new().set_index_metadata(&index_metadata).no_score(), &index_reader.schema())).unwrap();
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
            index_reader.search(&mut collector, &Query::new_all()).unwrap();
            collector.get_total_count()
        }
    };

    return Ok(json_response(status::Ok, json!({"count": count})));
}


pub fn view_search(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");

    // Lock index array
    let indices = system.indices.read().unwrap();

    // Get index
    let index = get_index_or_404!(indices, *index_name);
    let index_reader = index.store.reader();
    let index_metadata = index.metadata.read().unwrap();

    match json_from_request_body!(req) {
        Some(query_json) => {
            // Parse query
            let query = parse_query(query_json.as_object().unwrap().get("query").unwrap());
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
                    index_reader.search(&mut collector, &query.build(&QueryBuildContext::new().set_index_metadata(&index_metadata), &index_reader.schema())).unwrap();

                    // Convert hits into JSON
                    let mut hits = Vec::new();
                    for doc_match in collector.into_sorted_vec().iter().skip(from) {
                        let mut field_values = BTreeMap::new();

                        for &(ref field_name, field_ref) in fields.iter() {
                            let value = match index_reader.read_stored_field(field_ref, DocRef::from_u64(doc_match.doc_id())) {
                                Ok(Some(value)) => vec![value],
                                Ok(None) => vec![],
                                Err(_) => vec![],
                            };

                            field_values.insert(field_name.clone(), value);
                        }

                        hits.push(json!({
                            "_score": doc_match.score().unwrap(),
                            "fields": field_values,
                        }));
                    }

                    // TODO: {"took":5,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":4,"max_score":1.0,"hits":[{"_index":"wagtail","_type":"searchtests_searchtest_searchtests_searchtestchild","_id":"searchtests_searchtest:5380","_score":1.0,"fields":{"pk":["5380"]}},{"_index":"wagtail","_type":"searchtests_searchtest","_id":"searchtests_searchtest:5379","_score":1.0,"fields":{"pk":["5379"]}}]}}
                    Ok(json_response(status::Ok,
                                     json!({
                                         "hits": {
                                             "total": hits.len(),
                                             "hits": hits
                                        }})))
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
        None => Ok(json_response(status::BadRequest, json!({"message": "Missing query"}))),
    }
}
