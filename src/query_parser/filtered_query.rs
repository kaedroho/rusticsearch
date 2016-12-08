//! Parses "filtered" queries

use rustc_serialize::json::Json;
use kite::Query;

use query_parser::{QueryParseContext, QueryParseError, QueryBuilder, parse as parse_query};


#[derive(Debug)]
struct FilteredQueryBuilder {
    query: Option<Box<QueryBuilder>>,
    filter: Box<QueryBuilder>,
}


impl QueryBuilder for FilteredQueryBuilder {
    fn build(&self) -> Query {
        let query = match self.query {
            Some(ref query) => query.build(),
            None => Query::new_match_all(),
        };

        Query::Filter {
            query: Box::new(query),
            filter: Box::new(self.filter.build()),
        }
    }
}


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let mut query = None;

    let mut filter = None;
    let mut has_filter_key = false;

    for (key, value) in object.iter() {
        match key.as_ref() {
            "query" => {
                query = Some(try!(parse_query(&context.clone(), value)));
            }
            "filter" => {
                has_filter_key = true;
                filter = Some(try!(parse_query(&context.clone().no_score(), value)));
            }
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    if !has_filter_key {
        return Err(QueryParseError::ExpectedKey("filter"))
    }

    Ok(Box::new(FilteredQueryBuilder {
        query: query,
        filter: filter.unwrap(),
    }))
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use kite::{Term, Query, TermScorer};

    use query_parser::{QueryParseContext, QueryParseError};

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
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::MatchTerm {
                field: "the".to_string(),
                term: Term::String("query".to_string()),
                scorer: TermScorer::default(),
            }),
            filter: Box::new(Query::MatchTerm {
                field: "the".to_string(),
                term: Term::String("filter".to_string()),
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
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::new_match_all()),
            filter: Box::new(Query::MatchTerm {
                field: "the".to_string(),
                term: Term::String("filter".to_string()),
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

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Array
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        [
            \"foo\"
        ]
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        123
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));
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

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));
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

        assert_eq!(query.err(), Some(QueryParseError::ExpectedKey("filter")));
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

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));
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

        assert_eq!(query.err(), Some(QueryParseError::UnrecognisedKey("foo".to_string())));
    }
}
