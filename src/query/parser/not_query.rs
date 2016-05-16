use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError, parse as parse_query};
use query::parser::utils::parse_float;


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let sub_query = try!(parse_query(context, json));

    Ok(Query::Bool {
        must: vec![
            Query::MatchAll {
                boost: 1.0f64,
            }
        ],
        must_not: vec![sub_query],
        should: vec![],
        filter: vec![],
        minimum_should_match: 0,
        boost: 1.0f64,
    })
}
