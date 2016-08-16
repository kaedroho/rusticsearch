//! Parses "multi_match" queries

use rustc_serialize::json::Json;

use term::Term;
use analysis::Analyzer;
use mapping::FieldMapping;

use query::Query;
use query::term_matcher::TermMatcher;
use query::term_scorer::TermScorer;
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
        // Get mapping for field
        let field_mapping = match context.mappings {
            Some(mappings) => mappings.get_field(&field_name),
            None => None,
        };

        // Tokenise query string
        let tokens = match field_mapping {
            Some(ref field_mapping) => {
                field_mapping.process_value_for_query(Json::String(query.clone()))
            }
            None => {
                // TODO: Raise error?
                warn!("Unknown field: {}", field_name);

                FieldMapping::default().process_value_for_query(Json::String(query.clone()))
            }
        };

        let tokens = match tokens {
            Some(tokens) => tokens,
            None => {
                // Couldn't convert the passed in value into tokens
                // TODO: Raise error
                warn!("Unprocessable query: {}", query);

                vec![]
            }
        };

        let mut term_queries = Vec::new();
        for token in tokens {
            term_queries.push(Query::MatchTerm {
                field: field_name.clone(),
                term: token.term,
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            });
        }

        let mut field_query = match operator {
            Operator::Or => {
                Query::new_disjunction(term_queries)
            }
            Operator::And => {
                Query::new_conjunction(term_queries)
            }
        };

        // Add boost
        field_query = Query::new_boost(field_query, field_boost);

        field_queries.push(field_query);
    }

    let mut query = Query::new_disjunction_max(field_queries);

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
    fn test_multi_match_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: "bar".to_string(),
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                },
                Query::MatchTerm {
                    field: "baz".to_string(),
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                }
            ],
        }));
    }

    #[test]
    fn test_multi_term_multi_match_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"hello world\",
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Disjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: "bar".to_string(),
                            term: Term::String("hello".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: "bar".to_string(),
                            term: Term::String("world".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        }
                    ],
                },
                Query::Disjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("hello".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("world".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        }
                    ],
                }
            ],
        }));
    }

    #[test]
    fn test_with_boost() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": 2.0
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Boost {
            query: Box::new(Query::DisjunctionMax {
                queries: vec![
                    Query::MatchTerm {
                        field: "bar".to_string(),
                        term: Term::String("foo".to_string()),
                        matcher: TermMatcher::Exact,
                        scorer: TermScorer::default(),
                    },
                    Query::MatchTerm {
                        field: "baz".to_string(),
                        term: Term::String("foo".to_string()),
                        matcher: TermMatcher::Exact,
                        scorer: TermScorer::default(),
                    }
                ],
            }),
            boost: 2.0f64,
        }));
    }

    #[test]
    fn test_with_boost_integer() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": 2
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Boost {
            query: Box::new(Query::DisjunctionMax {
                queries: vec![
                    Query::MatchTerm {
                        field: "bar".to_string(),
                        term: Term::String("foo".to_string()),
                        matcher: TermMatcher::Exact,
                        scorer: TermScorer::default(),
                    },
                    Query::MatchTerm {
                        field: "baz".to_string(),
                        term: Term::String("foo".to_string()),
                        matcher: TermMatcher::Exact,
                        scorer: TermScorer::default(),
                    }
                ],
            }),
            boost: 2.0f64,
        }));
    }

    #[test]
    fn test_with_field_boost() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar^2\", \"baz^1.0\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Boost {
                    query: Box::new(Query::MatchTerm {
                        field: "bar".to_string(),
                        term: Term::String("foo".to_string()),
                        matcher: TermMatcher::Exact,
                        scorer: TermScorer::default(),
                    }),
                    boost: 2.0f64,
                },
                Query::MatchTerm {
                    field: "baz".to_string(),
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                }
            ],
        }));
    }

    #[test]
    fn test_with_and_operator() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo bar\",
            \"fields\": [\"baz\", \"quux\"],
            \"operator\": \"and\"
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Conjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("foo".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("bar".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        }
                    ],
                },
                Query::Conjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: "quux".to_string(),
                            term: Term::String("foo".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: "quux".to_string(),
                            term: Term::String("bar".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        }
                    ],
                }
            ],
        }));
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        \"foo\"
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

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
    fn test_gives_error_for_incorrect_query_type() {
        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": {
                \"foo\": \"bar\"
            },
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));

        // Array
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": [\"foo\"],
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));

        // Integer
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": 123,
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));

        // Float
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": 123.456,
            \"fields\": [\"bar\", \"baz\"]
        }        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));
    }

    #[test]
    fn test_gives_error_for_incorrect_fields_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": {
                \"value\": [\"bar\", \"baz\"]
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // Integer
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": 123
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // Float
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": 123.456
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));
    }

    #[test]
    fn test_gives_error_for_incorrect_boost_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": \"2\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": [2]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": {
                \"value\": 2
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));
    }

    #[test]
    fn test_gives_error_for_missing_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("query")));
    }

    #[test]
    fn test_gives_error_for_missing_fields() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("fields")));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
