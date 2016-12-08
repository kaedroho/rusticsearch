//! Parses "and" queries

use rustc_serialize::json::Json;
use kite::Query;

use query_parser::{QueryParseContext, QueryParseError, QueryBuilder, parse as parse_query};


#[derive(Debug)]
struct AndQueryBuilder {
    queries: Vec<Box<QueryBuilder>>,
}


impl QueryBuilder for AndQueryBuilder {
    fn build(&self) -> Query {
        let mut queries = Vec::new();

        for query in self.queries.iter() {
            queries.push(query.build());
        }

        Query::new_conjunction(queries)
    }
}


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let filters = try!(json.as_array().ok_or(QueryParseError::ExpectedArray));

    let mut queries = Vec::new();
    for filter in filters.iter() {
        queries.push(try!(parse_query(context, filter)));
    }

    Ok(Box::new(AndQueryBuilder {
        queries: queries
    }))
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use kite::{Term, Query, TermScorer};

    use query_parser::{QueryParseContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_and_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        [
            {
                \"term\": {
                    \"test\":  \"foo\"
                }
            },
            {
                \"term\": {
                    \"test\":  \"bar\"
                }
            }
        ]
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::Conjunction {
            queries: vec![
                Query::MatchTerm {
                    field: "test".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default(),
                },
                Query::MatchTerm {
                    field: "test".to_string(),
                    term: Term::String("bar".to_string()),
                    scorer: TermScorer::default(),
                },
            ],
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        \"hello\"
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // Integer
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        123
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // Float
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));
    }
}
