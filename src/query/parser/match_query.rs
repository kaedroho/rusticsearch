use std::borrow::Cow;

use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError};


pub fn parse(context: Cow<QueryParseContext>, json: &Json) -> Result<Query, QueryParseError> {
    Ok(Query::MatchNone)
}
