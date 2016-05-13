pub mod match_query;

use std::borrow::Cow;

use rustc_serialize::json::Json;

use query::Query;


#[derive(Debug, PartialEq, Clone)]
pub struct QueryParseContext {

}


#[derive(Debug, PartialEq)]
pub enum QueryParseError {
    UnrecognisedQueryType(String),
}


fn get_query_parser(query_name: &str) -> Option<fn(Cow<QueryParseContext>, &Json) -> Result<Query, QueryParseError>> {
    match query_name {
        "match" => Some(match_query::parse),
        _ => None
    }
}


pub fn parse(context: Cow<QueryParseContext>, json: &Json) -> Result<Query, QueryParseError> {
    let query_type = "match".to_owned();

    match get_query_parser(&query_type) {
        Some(parse) => parse(context, json),
        None => Err(QueryParseError::UnrecognisedQueryType(query_type)),
    }
}
