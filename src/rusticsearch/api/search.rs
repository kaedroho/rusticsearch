use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;
use rocket::State;
use rocket_contrib::JSON;
use kite::document::DocRef;
use kite::collectors::top_score::TopScoreCollector;
use kite::collectors::total_count::TotalCountCollector;

use system::System;
use query_parser::{QueryBuildContext, parse as parse_query};


#[post("/<index_name>/_count", data = "<query_json>")]
pub fn count(index_name: &str, query_json: JSON<Value>, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Get index
    let cluster_metadata = system.metadata.read().unwrap();
    let index = get_index_or_404!(cluster_metadata, index_name);
    let index_reader = index.store.reader();
    let index_metadata = index.metadata.read().unwrap();

    let count = {
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
                return Some(JSON(json!({"message": "Query error"})));  // TODO 400 error
            }
        }
    };

    Some(JSON(json!({"count": count})))
}


#[derive(Debug, FromForm)]
pub struct SearchParams {
    pub from: usize,
    pub size: usize,
    pub fields: Option<String>,
    // terminate_after
    // explain
    // version
    // timeout
    // fielddata_fields
    // track_scores
    // stats
    // suggest_field
}

impl Default for SearchParams {
    fn default() -> SearchParams {
        SearchParams {
            from: 0,
            size: 10,
            fields: None,
        }
    }
}


#[post("/<index_name>/_search?<params>", data = "<query_json>")]
pub fn search(index_name: &str, params: SearchParams, query_json: JSON<Value>, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Get index
    let cluster_metadata = system.metadata.read().unwrap();
    let index = get_index_or_404!(cluster_metadata, index_name);
    let index_reader = index.store.reader();
    let index_metadata = index.metadata.read().unwrap();

    // Parse query
    let query = parse_query(query_json.as_object().unwrap().get("query").unwrap());
    debug!("{:#?}", query);

    match query {
        Ok(query) => {
            let mut fields = Vec::new();
            if let Some(value) = params.fields {
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


            // Do the search
            let mut collector = TopScoreCollector::new(params.from + params.size);
            index_reader.search(&mut collector, &query.build(&QueryBuildContext::new().set_index_metadata(&index_metadata), &index_reader.schema())).unwrap();

            // Convert hits into JSON
            let mut hits = Vec::new();
            for doc_match in collector.into_sorted_vec().iter().skip(params.from) {
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
            Some(JSON(json!({
                "hits": {
                    "total": hits.len(),
                    "hits": hits
                }
            })))
        }
        Err(_) => {
            // TODO: What specifically is bad about the Query?
            Some(JSON(json!({"message": "Query error"})))  // TODO 400 error
        }
    }
}
