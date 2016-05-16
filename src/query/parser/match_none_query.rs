use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Get configuration
    for (key, value) in object.iter() {
        match &key[..] {
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    Ok(Query::MatchNone)
}
