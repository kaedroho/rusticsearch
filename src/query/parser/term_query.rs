use rustc_serialize::json::Json;

use analysis::Analyzer;
use Value;

use query::{Query, TermMatcher};
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::utils::{parse_string, parse_float, Operator, parse_operator};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey)
    };

    let object = object.get(field_name).unwrap();

    // Get configuration
    let mut value: Option<Value> = None;
    let mut boost = 1.0f64;

    match *object {
        Json::Object(ref inner_object) => {
            for (key, val) in inner_object.iter() {
                match key.as_ref() {
                    "value" => {
                        value = Some(Value::from_json(val));
                    }
                    "boost" => {
                        boost = try!(parse_float(val));
                    }
                    _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
                }
            }
        }
        _ => value = Some(Value::from_json(object)),
    }

    match value {
        Some(value) => {
            Ok(Query::MatchTerm {
                field: field_name.clone(),
                value: value,
                matcher: TermMatcher::Exact,
                boost: boost,
            })
        }
        None => Err(QueryParseError::ExpectedKey("value"))
    }
}
