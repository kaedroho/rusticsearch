pub mod utils;
pub mod match_query;
pub mod multi_match_query;
pub mod match_all_query;
pub mod match_none_query;
pub mod filtered_query;
pub mod prefix_query;

use rustc_serialize::json::Json;

use query::Query;


#[derive(Debug, PartialEq, Clone)]
pub struct QueryParseContext {
    score_required: bool,
}


impl Default for QueryParseContext {
    fn default() -> QueryParseContext {
        QueryParseContext {
            score_required: true
        }
    }
}


impl QueryParseContext {
    #[inline]
    pub fn no_score(mut self) -> QueryParseContext {
        self.score_required = false;
        self
    }
}


#[derive(Debug, PartialEq)]
pub enum QueryParseError {
    UnrecognisedQueryType(String),
    UnrecognisedKey(String),
    ExpectedKey(&'static str),
    ExpectedObject,
    ExpectedArray,
    ExpectedString,
    ExpectedFloat,
    ExpectedObjectOrString,
    ExpectedSingleKey,
    InvalidOperator,
}


fn get_query_parser(query_name: &str) -> Option<fn(&QueryParseContext, &Json) -> Result<Query, QueryParseError>> {
    match query_name {
        "match" => Some(match_query::parse),
        "multi_match" => Some(multi_match_query::parse),
        "match_all" => Some(match_all_query::parse),
        "match_none" => Some(match_none_query::parse),
        "filtered" => Some(filtered_query::parse),
        "prefix" => Some(prefix_query::parse),
        _ => None
    }
}


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let query_type = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey)
    };

    match get_query_parser(&query_type) {
        Some(parse) => parse(context, object.get(query_type).unwrap()),
        None => Err(QueryParseError::UnrecognisedQueryType(query_type.clone())),
    }
}
