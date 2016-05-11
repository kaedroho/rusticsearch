pub mod match_query;

use rustc_serialize::json::Json;

use query::Query;


#[derive(Debug, PartialEq)]
pub enum QueryParseError {
}


pub fn parse(json: &Json) -> Result<Query, QueryParseError> {
    Ok(Query::MatchNone)
}
