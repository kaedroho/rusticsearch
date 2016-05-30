use rustc_serialize::json::Json;

use analysis::Analyzer;
use term::Term;

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
        for token in Analyzer::Standard.run(query.clone()) {
            term_queries.push(Query::MatchTerm {
                field: field_name.clone(),
                term: token.term,
                matcher: TermMatcher::Exact,
                boost: 1.0f64,
            });
        }

        let mut field_query = match operator {
            Operator::Or => {
                Query::Bool {
                    must: vec![],
                    must_not: vec![],
                    should: term_queries,
                    filter: vec![],
                    minimum_should_match: 1,
                }
            }
            Operator::And => {
                Query::Bool {
                    must: term_queries,
                    must_not: vec![],
                    should: vec![],
                    filter: vec![],
                    minimum_should_match: 0,
                }
            }
        };

        // Add boost
        if field_boost != 1.0f64 {
            field_query = Query::BoostScore {
                query: Box::new(field_query),
                boost: field_boost,
            };
        }

        field_queries.push(field_query);
    }

    let mut query = Query::DisjunctionMax {
        queries: field_queries,
    };

    // Add boost
    if boost != 1.0f64 {
        query = Query::BoostScore {
            query: Box::new(query),
            boost: boost,
        };
    }

    return Ok(query);
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use term::Term;
    use query::{Query, TermMatcher};
    use query::parser::{QueryParseContext, QueryParseError};
    use index::Index;

    use super::parse;

    #[test]
    fn test_multi_match_query() {
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Bool {
                    must: vec![],
                    must_not: vec![],
                    should: vec![
                        Query::MatchTerm {
                            field: "bar".to_string(),
                            term: Term::String("foo".to_string()),
                            matcher: TermMatcher::Exact,
                            boost: 1.0f64,
                        }
                    ],
                    filter: vec![],
                    minimum_should_match: 1,
                },
                Query::Bool {
                    must: vec![],
                    must_not: vec![],
                    should: vec![
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("foo".to_string()),
                            matcher: TermMatcher::Exact,
                            boost: 1.0f64,
                        }
                    ],
                    filter: vec![],
                    minimum_should_match: 1,
                }
            ],
        }));
    }

    #[test]
    fn test_multi_term_multi_match_query() {
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"hello world\",
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Bool {
                    must: vec![],
                    must_not: vec![],
                    should: vec![
                        Query::MatchTerm {
                            field: "bar".to_string(),
                            term: Term::String("hello".to_string()),
                            matcher: TermMatcher::Exact,
                            boost: 1.0f64,
                        },
                        Query::MatchTerm {
                            field: "bar".to_string(),
                            term: Term::String("world".to_string()),
                            matcher: TermMatcher::Exact,
                            boost: 1.0f64,
                        }
                    ],
                    filter: vec![],
                    minimum_should_match: 1,
                },
                Query::Bool {
                    must: vec![],
                    must_not: vec![],
                    should: vec![
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("hello".to_string()),
                            matcher: TermMatcher::Exact,
                            boost: 1.0f64,
                        },
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("world".to_string()),
                            matcher: TermMatcher::Exact,
                            boost: 1.0f64,
                        }
                    ],
                    filter: vec![],
                    minimum_should_match: 1,
                }
            ],
        }));
    }

    #[test]
    fn test_with_boost() {
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": 2.0
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::BoostScore {
            query: Box::new(Query::DisjunctionMax {
                queries: vec![
                    Query::Bool {
                        must: vec![],
                        must_not: vec![],
                        should: vec![
                            Query::MatchTerm {
                                field: "bar".to_string(),
                                term: Term::String("foo".to_string()),
                                matcher: TermMatcher::Exact,
                                boost: 1.0f64,
                            }
                        ],
                        filter: vec![],
                        minimum_should_match: 1,
                    },
                    Query::Bool {
                        must: vec![],
                        must_not: vec![],
                        should: vec![
                            Query::MatchTerm {
                                field: "baz".to_string(),
                                term: Term::String("foo".to_string()),
                                matcher: TermMatcher::Exact,
                                boost: 1.0f64,
                            }
                        ],
                        filter: vec![],
                        minimum_should_match: 1,
                    }
                ],
            }),
            boost: 2.0f64,
        }));
    }

    #[test]
    fn test_with_boost_integer() {
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": 2
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::BoostScore {
            query: Box::new(Query::DisjunctionMax {
                queries: vec![
                    Query::Bool {
                        must: vec![],
                        must_not: vec![],
                        should: vec![
                            Query::MatchTerm {
                                field: "bar".to_string(),
                                term: Term::String("foo".to_string()),
                                matcher: TermMatcher::Exact,
                                boost: 1.0f64,
                            }
                        ],
                        filter: vec![],
                        minimum_should_match: 1,
                    },
                    Query::Bool {
                        must: vec![],
                        must_not: vec![],
                        should: vec![
                            Query::MatchTerm {
                                field: "baz".to_string(),
                                term: Term::String("foo".to_string()),
                                matcher: TermMatcher::Exact,
                                boost: 1.0f64,
                            }
                        ],
                        filter: vec![],
                        minimum_should_match: 1,
                    }
                ],
            }),
            boost: 2.0f64,
        }));
    }

    #[test]
    fn test_with_field_boost() {
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar^2\", \"baz^1.0\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::BoostScore {
                    query: Box::new(Query::Bool {
                        must: vec![],
                        must_not: vec![],
                        should: vec![
                            Query::MatchTerm {
                                field: "bar".to_string(),
                                term: Term::String("foo".to_string()),
                                matcher: TermMatcher::Exact,
                                boost: 1.0f64,
                            }
                        ],
                        filter: vec![],
                        minimum_should_match: 1,
                    }),
                    boost: 2.0f64,
                },
                Query::Bool {
                    must: vec![],
                    must_not: vec![],
                    should: vec![
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("foo".to_string()),
                            matcher: TermMatcher::Exact,
                            boost: 1.0f64,
                        }
                    ],
                    filter: vec![],
                    minimum_should_match: 1,
                }
            ],
        }));
    }

    #[test]
    fn test_with_and_operator() {
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"operator\": \"and\"
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Bool {
                    must: vec![
                        Query::MatchTerm {
                            field: "bar".to_string(),
                            term: Term::String("foo".to_string()),
                            matcher: TermMatcher::Exact,
                            boost: 1.0f64,
                        }
                    ],
                    must_not: vec![],
                    should: vec![],
                    filter: vec![],
                    minimum_should_match: 0,
                },
                Query::Bool {
                    must: vec![
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("foo".to_string()),
                            matcher: TermMatcher::Exact,
                            boost: 1.0f64,
                        }
                    ],
                    must_not: vec![],
                    should: vec![],
                    filter: vec![],
                    minimum_should_match: 0,
                }
            ],
        }));
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        \"foo\"
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Array
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        [
            \"foo\"
        ]
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        123
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_incorrect_query_type() {
        // Object
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": {
                \"foo\": \"bar\"
            },
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));

        // Array
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": [\"foo\"],
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));

        // Integer
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": 123,
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));

        // Float
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": 123.456,
            \"fields\": [\"bar\", \"baz\"]
        }        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));
    }

    #[test]
    fn test_gives_error_for_incorrect_fields_type() {
        // String
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // Object
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": {
                \"value\": [\"bar\", \"baz\"]
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // Integer
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": 123
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // Float
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
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
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": \"2\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": [2]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
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
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("query")));
    }

    #[test]
    fn test_gives_error_for_missing_fields() {
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("fields")));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
