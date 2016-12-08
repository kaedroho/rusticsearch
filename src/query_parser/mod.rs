//! Parses Elasticsearch Query DSL

pub mod utils;
pub mod match_query;
pub mod multi_match_query;
pub mod match_all_query;
pub mod match_none_query;
pub mod filtered_query;
pub mod terms_query;
pub mod term_query;
pub mod prefix_query;
pub mod and_query;
pub mod or_query;
pub mod not_query;

use std::fmt::Debug;

use rustc_serialize::json::Json;
use kite::Query;

use mapping::MappingRegistry;


#[derive(Debug, Clone)]
pub struct QueryParseContext<'a> {
    pub mappings: Option<&'a MappingRegistry>,
    score_required: bool,
}


impl<'a> QueryParseContext<'a> {
    pub fn new() -> QueryParseContext<'a> {
        QueryParseContext {
            mappings: None,
            score_required: true
        }
    }

    #[inline]
    pub fn set_mappings(mut self, mappings: &'a MappingRegistry) -> QueryParseContext<'a> {
        self.mappings = Some(mappings);
        self
    }

    #[inline]
    pub fn no_score(mut self) -> QueryParseContext<'a> {
        self.score_required = false;
        self
    }
}


#[derive(Debug, PartialEq)]
pub enum QueryParseError {
    UnrecognisedQueryType(String),
    FieldDoesntExist(String),
    UnrecognisedKey(String),
    ExpectedKey(&'static str),
    ExpectedObject,
    ExpectedArray,
    ExpectedString,
    ExpectedFloat,
    ExpectedObjectOrString,
    InvalidValue,
    ExpectedSingleKey,
    InvalidOperator,
}


pub trait QueryBuilder: Debug {
    fn build(&self) -> Query;
}


fn get_query_parser(query_name: &str) -> Option<fn(&QueryParseContext, &Json) -> Result<Box<QueryBuilder>, QueryParseError>> {
    match query_name {
        "match" => Some(match_query::parse),
        "multi_match" => Some(multi_match_query::parse),
        "match_all" => Some(match_all_query::parse),
        "match_none" => Some(match_none_query::parse),
        "filtered" => Some(filtered_query::parse),
        "terms" => Some(terms_query::parse),
        "in" => Some(terms_query::parse),
        "term" => Some(term_query::parse),
        "prefix" => Some(prefix_query::parse),
        "and" => Some(and_query::parse),
        "or" => Some(or_query::parse),
        "not" => Some(not_query::parse),
        _ => None
    }
}


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
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
