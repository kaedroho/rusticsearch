use rustc_serialize::json::Json;

use query::Query;
use query::parser::QueryParseError;


pub fn parse(json: &Json) -> Result<Query, QueryParseError> {
    Ok(Query::MatchNone)
}
