use rustc_serialize::json::Json;

use analysis::Analyzer;
use Term;

use query::{Query, TermMatcher};
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::utils::{parse_string, parse_float, Operator, parse_operator};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Prefix queries are very similar to term queries except that they will also match prefixes
    // of terms
    //
    // {
    //     "foo": "bar"
    // }
    //
    // {
    //     "foo": {
    //         "query": "bar",
    //         "boost": 2.0
    //     }
    // }
    //
    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey)
    };

    let object = object.get(field_name).unwrap();

    // Get configuration
    let mut value: Option<&Json> = None;
    let mut boost = 1.0f64;

    match *object {
        Json::String(ref string) => value = Some(object),
        Json::Object(ref inner_object) => {
            for (key, val) in inner_object.iter() {
                match key.as_ref() {
                    "value" => {
                        value = Some(val);
                    }
                    "prefix" => {
                        value = Some(val);
                    }
                    "boost" => {
                        boost = try!(parse_float(val));
                    }
                    _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
                }
            }
        }
        _ => return Err(QueryParseError::ExpectedObjectOrString),
    }

    match value {
        Some(value) => {
            Ok(Query::MatchTerm {
                field: field_name.clone(),
                term: Term::from_json(value),
                matcher: TermMatcher::Prefix,
                boost: boost,
            })
        }
        None => Err(QueryParseError::ExpectedKey("value"))
    }
}
