use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::utils::parse_float;


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Get configuration
    let mut boost = 1.0f64;

    for (key, value) in object.iter() {
        match &key[..] {
            "boost" => {
                boost = try!(parse_float(value));
            }
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    Ok(Query::MatchAll {
        boost: boost,
    })
}
