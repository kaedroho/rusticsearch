use rustc_serialize::json::Json;

use analysis::Analyzer;
use term::Term;
use mapping::FieldMapping;
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

    // Get mapping for field
    let field_mapping = context.index.get_field_mapping_by_name(field_name);

    // Get configuration
    let mut query = Json::Null;
    let mut boost = 1.0f64;
    let mut operator = Operator::Or;

    match object[field_name] {
        Json::String(ref string) => query = object[field_name].clone(),
        Json::Object(ref inner_object) => {
            let mut has_query_key = false;

            for (key, value) in object.iter() {
                match key.as_ref() {
                    "query" => {
                        has_query_key = true;
                        query = value.clone();
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

    // Tokenise query string
    let tokens = match field_mapping {
        Some(ref field_mapping) => {
            field_mapping.process_value_for_query(query.clone())
        }
        None => {
            // TODO: Raise error?
            warn!("Unknown field: {}", field_name);

            FieldMapping::default().process_value_for_query(query.clone())
        }
    };

    let tokens = match tokens {
        Some(tokens) => tokens,
        None => {
            // Couldn't convert the passed in value into tokens
            // TODO: Raise error
            warn!("Unprocessable query: {}", query);

            return Ok(Query::MatchNone);
        }
    };

    // Create a term query for each token
    let mut sub_queries = Vec::new();
    for token in tokens {
        sub_queries.push(Query::MatchTerm {
            field: field_name.clone(),
            term: token.term,
            matcher: TermMatcher::Exact,
            boost: 1.0f64,
        });
    }

    // Combine the term queries
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
