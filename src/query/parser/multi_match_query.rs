use rustc_serialize::json::Json;

use analysis::Analyzer;

use query::{Query, TermMatcher};
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::utils::{parse_string, parse_float, Operator, parse_operator, parse_field_and_boost};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Get configuration
    let mut fields_with_boosts = Vec::new();
    let mut query = String::new();
    let mut boost = 1.0f64;
    let mut operator = Operator::Or;

    let mut has_fields_key = false;
    let mut has_query_key = false;

    for (key, val) in object.iter() {
        match key.as_ref() {
            "fields" => {
                has_fields_key = true;

                match *val {
                    Json::Array(ref array) => {
                        for field in array.iter() {
                            fields_with_boosts.push(try!(parse_field_and_boost(field)));
                        }
                    }
                    _ => return Err(QueryParseError::ExpectedArray)
                }
            }
            "query" => {
                has_query_key = true;
                query = try!(parse_string(val));
            }
            "boost" => {
                boost = try!(parse_float(val));
            }
            "operator" => {
                operator = try!(parse_operator(val))
            }
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    if !has_fields_key {
        return Err(QueryParseError::ExpectedKey("fields"))
    }

    if !has_query_key {
        return Err(QueryParseError::ExpectedKey("query"))
    }

    // Convert query string into term query objects
    let mut field_queries = Vec::new();
    for (field_name, field_boost) in fields_with_boosts {
        let mut term_queries = Vec::new();
        for term in Analyzer::Standard.run(query.clone()) {
            term_queries.push(Query::MatchTerm {
                fields: vec![field_name.clone()],
                value: term,
                matcher: TermMatcher::Exact,
                boost: 1.0f64,
            });
        }

        field_queries.push(match operator {
            Operator::Or => {
                Query::Bool {
                    must: vec![],
                    must_not: vec![],
                    should: term_queries,
                    filter: vec![],
                    minimum_should_match: 1,
                    boost: field_boost,
                }
            }
            Operator::And => {
                Query::Bool {
                    must: term_queries,
                    must_not: vec![],
                    should: vec![],
                    filter: vec![],
                    minimum_should_match: 0,
                    boost: field_boost,
                }
            }
        });
    }

    Ok(Query::DisjunctionMax {
        queries: field_queries,
        boost: boost,
    })
}
