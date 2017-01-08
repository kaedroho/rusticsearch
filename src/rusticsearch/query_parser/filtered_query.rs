//! Parses "filtered" queries

use rustc_serialize::json::Json;
use kite::Query;
use kite::schema::Schema;

use query_parser::{QueryBuildContext, QueryParseError, QueryBuilder, parse as parse_query};


#[derive(Debug)]
struct FilteredQueryBuilder {
    query: Option<Box<QueryBuilder>>,
    filter: Box<QueryBuilder>,
}


impl QueryBuilder for FilteredQueryBuilder {
    fn build(&self, context: &QueryBuildContext, schema: &Schema) -> Query {
        let query = match self.query {
            Some(ref query) => query.build(context, schema),
            None => Query::new_match_all(),
        };

        Query::Filter {
            query: Box::new(query),
            filter: Box::new(self.filter.build(&context.clone().no_score(), schema)),
        }
    }
}


pub fn parse(json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let mut query = None;

    let mut filter = None;
    let mut has_filter_key = false;

    for (key, value) in object.iter() {
        match key.as_ref() {
            "query" => {
                query = Some(try!(parse_query(value)));
            }
            "filter" => {
                has_filter_key = true;
                filter = Some(try!(parse_query(value)));
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
    use kite::schema::{Schema, FieldType, FIELD_INDEXED};

    use query_parser::{QueryBuildContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_filtered_query() {
        let mut schema = Schema::new();
        let the_field = schema.add_field("the".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
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
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::MatchTerm {
                field: the_field,
                term: Term::String("query".to_string()),
                scorer: TermScorer::default(),
            }),
            filter: Box::new(Query::MatchTerm {
                field: the_field,
                term: Term::String("filter".to_string()),
                scorer: TermScorer::default(),
            }),
        }))
    }

    #[test]
    fn test_without_sub_query() {
        let mut schema = Schema::new();
        let the_field = schema.add_field("the".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"filter\": {
                \"term\": {
                    \"the\": \"filter\"
                }
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::new_match_all()),
            filter: Box::new(Query::MatchTerm {
                field: the_field,
                term: Term::String("filter".to_string()),
                scorer: TermScorer::default(),
            }),
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&Json::from_str("
        \"hello\"
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Array
        let query = parse(&Json::from_str("
        [
            \"foo\"
        ]
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&Json::from_str("
        123
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&Json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_invalid_query() {
        let query = parse(&Json::from_str("
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
        let query = parse(&Json::from_str("
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
        let query = parse(&Json::from_str("
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
        let query = parse(&Json::from_str("
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
