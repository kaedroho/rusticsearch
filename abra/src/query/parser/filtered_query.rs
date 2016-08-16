//! Parses "filtered" queries

use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::{parse as parse_query};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let mut query = Query::MatchAll;

    let mut filter = Query::MatchNone;
    let mut has_filter_key = false;

    for (key, value) in object.iter() {
        match key.as_ref() {
            "query" => {
                query = try!(parse_query(&context.clone(), value));
            }
            "filter" => {
                has_filter_key = true;
                filter = try!(parse_query(&context.clone().no_score(), value));
            }
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    if !has_filter_key {
        return Err(QueryParseError::ExpectedKey("filter"))
    }

    return Ok(Query::Filter {
        query: Box::new(query),
        filter: Box::new(filter),
    })
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
    fn test_filtered_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": {
                \"term\": {
                    \"the\": \"query\"
                }
            },
            \"filter\": {
                \"term\": {
                    \"the\": \"filter\"
                }
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::MatchTerm {
                field: "the".to_string(),
                term: Term::String("query".to_string()),
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            }),
            filter: Box::new(Query::MatchTerm {
                field: "the".to_string(),
                term: Term::String("filter".to_string()),
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            }),
        }))
    }

    #[test]
    fn test_without_sub_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"filter\": {
                \"term\": {
                    \"the\": \"filter\"
                }
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::MatchAll),
            filter: Box::new(Query::MatchTerm {
                field: "the".to_string(),
                term: Term::String("filter".to_string()),
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            }),
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        \"hello\"
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
    fn test_gives_error_for_invalid_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"filter\": {
                \"term\": {
                    \"the\": \"filter\"
                }
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_missing_filter() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": {
                \"term\": {
                    \"the\": \"query\"
                }
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("filter")));
    }

    #[test]
    fn test_gives_error_for_invalid_filter() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": {
                \"term\": {
                    \"the\": \"query\"
                }
            },
            \"filter\": \"foo\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_unexpected_key() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": {
                \"term\": {
                    \"the\": \"query\"
                }
            },
            \"filter\": {
                \"term\": {
                    \"the\": \"filter\"
                }
            },
            \"foo\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::UnrecognisedKey("foo".to_string())));
    }
}
