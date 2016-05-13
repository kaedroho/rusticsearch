use rustc_serialize::json::Json;

use analysis::Analyzer;

use query::{Query, TermMatcher};
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::utils::{parse_string, parse_float, Operator, parse_operator};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Match queries are single-key objects. The key is the field name, the value is either a
    // string or a sub-object with extra configuration:
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
    let mut query = String::new();
    let mut boost = 1.0f64;
    let mut operator = Operator::Or;

    match object[field_name] {
        Json::String(ref string) => query = string.clone(),
        Json::Object(ref inner_object) => {
            let mut has_query_key = false;

            for (key, value) in object.iter() {
                match key.as_ref() {
                    "query" => {
                        has_query_key = true;
                        query = try!(parse_string(value));
                    }
                    "boost" => {
                        boost = try!(parse_float(value));
                    }
                    "operator" => {
                        operator = try!(parse_operator(value))
                    }
                    _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
                }
            }

            if !has_query_key {
                return Err(QueryParseError::ExpectedKey("query"))
            }
        }
        _ => return Err(QueryParseError::ExpectedObjectOrString),
    }

    // Convert query string into term query objects
    let mut sub_queries = Vec::new();
    for term in Analyzer::Standard.run(query) {
        sub_queries.push(Query::MatchTerm {
            fields: vec![field_name.clone()],
            value: term,
            matcher: TermMatcher::Exact,
            boost: 1.0f64,
        });
    }

    match operator {
        Operator::Or => {
            Ok(Query::Bool {
                must: vec![],
                must_not: vec![],
                should: sub_queries,
                filter: vec![],
                minimum_should_match: 1,
                boost: boost,
            })
        }
        Operator::And => {
            Ok(Query::Bool {
                must: sub_queries,
                must_not: vec![],
                should: vec![],
                filter: vec![],
                minimum_should_match: 0,
                boost: boost,
            })
        }
    }
}
