use rustc_serialize::json::Json;

use analysis::Analyzer;

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

    // Get configuration
    let mut value = String::new();
    let mut boost = 1.0f64;

    match object[field_name] {
        Json::String(ref string) => value = string.clone(),
        Json::Object(ref inner_object) => {
            let mut has_value_key = false;

            for (key, val) in object.iter() {
                match key.as_ref() {
                    "value" => {
                        has_value_key = true;
                        value = try!(parse_string(val));
                    }
                    "prefix" => {
                        has_value_key = true;
                        value = try!(parse_string(val));
                    }
                    "boost" => {
                        boost = try!(parse_float(val));
                    }
                    _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
                }
            }

            if !has_value_key {
                return Err(QueryParseError::ExpectedKey("value"))
            }
        }
        _ => return Err(QueryParseError::ExpectedObjectOrString),
    }

    Ok(Query::MatchTerm {
        fields: vec![field_name.clone()],
        value: value,
        matcher: TermMatcher::Prefix,
        boost: boost,
    })
}
