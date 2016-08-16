//! Parses "match" queries

use rustc_serialize::json::Json;

use term::Term;
use analysis::Analyzer;
use mapping::FieldMapping;

use query::Query;
use query::term_matcher::TermMatcher;
use query::term_scorer::TermScorer;
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::utils::{parse_string, parse_float, Operator, parse_operator};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey)
    };

    // Get mapping for field
    let field_mapping = match context.mappings {
        Some(mappings) => mappings.get_field(field_name),
        None => None,
    };

    // Get configuration
    let mut query = Json::Null;
    let mut boost = 1.0f64;
    let mut operator = Operator::Or;

    match object[field_name] {
        Json::String(ref string) => query = object[field_name].clone(),
        Json::Object(ref inner_object) => {
            let mut has_query_key = false;

            for (key, value) in inner_object.iter() {
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
            scorer: TermScorer::default(),
        });
    }

    // Combine the term queries
    let mut query = match operator {
        Operator::Or => {
            Query::new_disjunction(sub_queries)
        }
        Operator::And => {
            Query::new_conjunction(sub_queries)
        }
    };

    // Add boost
    query = Query::new_boost(query, boost);

    return Ok(query);
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use term::Term;
    use query::Query;
    use query::term_matcher::TermMatcher;
    use query::term_scorer::TermScorer;
    use query::parser::{QueryParseContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_match_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchTerm {
            field: "foo".to_string(),
            term: Term::String("bar".to_string()),
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default(),
        }))
    }

    #[test]
    fn test_multi_term_match_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar baz\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Disjunction {
            queries: vec![
                Query::MatchTerm {
                    field: "foo".to_string(),
                    term: Term::String("bar".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                },
                Query::MatchTerm {
                    field: "foo".to_string(),
                    term: Term::String("baz".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                }
            ],
        }))
    }

    #[test]
    fn test_simple_multi_term_match_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": \"bar baz\"
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Disjunction {
            queries: vec![
                Query::MatchTerm {
                    field: "foo".to_string(),
                    term: Term::String("bar".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                },
                Query::MatchTerm {
                    field: "foo".to_string(),
                    term: Term::String("baz".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                }
            ],
        }))
    }

    #[test]
    fn test_with_boost() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": 2.0
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Boost {
            query: Box::new(Query::MatchTerm {
                field: "foo".to_string(),
                term: Term::String("bar".to_string()),
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            }),
            boost: 2.0f64,
        }))
    }

    #[test]
    fn test_with_boost_integer() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": 2
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Boost {
            query: Box::new(Query::MatchTerm {
                field: "foo".to_string(),
                term: Term::String("bar".to_string()),
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            }),
            boost: 2.0f64,
        }))
    }

    #[test]
    fn test_with_and_operator() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar baz\",
                \"operator\": \"and\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Conjunction {
            queries: vec![
                Query::MatchTerm {
                    field: "foo".to_string(),
                    term: Term::String("bar".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                },
                Query::MatchTerm {
                    field: "foo".to_string(),
                    term: Term::String("baz".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                }
            ],
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // Array
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        [
            \"foo\"
        ]
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        123
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_incorrect_boost_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": \"2\"
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": [2]
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": {
                    \"value\": 2
                }
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));
    }

    #[test]
    fn test_gives_error_for_missing_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("query")));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\"
            },
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedSingleKey));
    }

    #[test]
    fn test_gives_error_for_extra_inner_key() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"hello\": \"world\"
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
