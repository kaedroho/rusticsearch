use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::{parse as parse_query};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Filtered queries contain two keys. The "filter" and the "query". Both keys are sub-queries
    // except that the one specified in "filter" has its score ignored
    //
    // {
    //     "filter": {
    //         "term": {
    //             "foo": "bar"
    //         }
    //     },
    //     "query": {
    //         "match": {
    //             "baz": "quux"
    //         }
    //    }
    // }
    //
    let mut query = Query::MatchAll{
        boost: 1.0f64,
    };

    let mut filter = Query::MatchNone;
    let mut has_filter_key = false;

    for (key, value) in object.iter() {
        match key.as_ref() {
            "query" => {
                query = try!(parse_query(&context.clone(), value));
            }
            "filter" => {
                has_filter_key = true;
                filter = try!(parse_query(&context.clone().no_score(), value));
            }
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    if !has_filter_key {
        return Err(QueryParseError::ExpectedKey("filter"))
    }

    return Ok(Query::Bool {
        must: vec![query],
        must_not: vec![],
        should: vec![],
        filter: vec![filter],
        minimum_should_match: 0,
        boost: 1.0f64,
    })
}
