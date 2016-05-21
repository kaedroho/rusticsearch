use rustc_serialize::json::Json;

use term::Term;

use query::{Query, TermMatcher};
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::utils::parse_float;


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey)
    };

    let object = object.get(field_name).unwrap();

    // Get configuration
    let mut term: Option<Term> = None;
    let mut boost = 1.0f64;

    match *object {
        Json::Object(ref inner_object) => {
            for (key, val) in inner_object.iter() {
                match key.as_ref() {
                    "value" => {
                        term = Some(Term::from_json(val));
                    }
                    "boost" => {
                        boost = try!(parse_float(val));
                    }
                    _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
                }
            }
        }
        _ => term = Some(Term::from_json(object)),
    }

    match term {
        Some(term) => {
            Ok(Query::MatchTerm {
                field: field_name.clone(),
                term: term,
                matcher: TermMatcher::Exact,
                boost: boost,
            })
        }
        None => Err(QueryParseError::ExpectedKey("value"))
    }
}
