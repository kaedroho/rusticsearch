//! Parses "filtered" queries

use rustc_serialize::json::Json;
use abra::Query;

use query_parser::{QueryParseContext, QueryParseError};
use query_parser::{parse as parse_query};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let mut query = Query::new_match_all();

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

    use abra::{Term, Query, TermMatcher, TermScorer};
    use abra::schema::{Schema, SchemaWrite, FieldType, FieldRef};

    use query_parser::{QueryParseContext, QueryParseError};
    use mapping::{MappingRegistry, Mapping, FieldMapping};

    use super::parse;

    fn make_one_field_schema() -> (Schema, FieldRef) {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text).unwrap();
        (schema, foo_field)
    }

    #[test]
    fn test_filtered_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": {
                \"term\": {
                    \"foo\": \"query\"
                }
            },
            \"filter\": {
                \"term\": {
                    \"foo\": \"filter\"
                }
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::MatchTerm {
                field: foo_field,
                term: Term::String("query".to_string()),
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            }),
            filter: Box::new(Query::MatchTerm {
                field: foo_field,
                term: Term::String("filter".to_string()),
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            }),
        }))
    }

    #[test]
    fn test_without_sub_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"filter\": {
                \"term\": {
                    \"foo\": \"filter\"
                }
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::new_match_all()),
            filter: Box::new(Query::MatchTerm {
                field: foo_field,
                term: Term::String("filter".to_string()),
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            }),
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        let (schema, foo_field) = make_one_field_schema();

        // String
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        \"hello\"
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Array
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        [
            \"foo\"
        ]
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        123
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_invalid_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"filter\": {
                \"term\": {
                    \"foo\": \"filter\"
                }
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_missing_filter() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": {
                \"term\": {
                    \"foo\": \"query\"
                }
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("filter")));
    }

    #[test]
    fn test_gives_error_for_invalid_filter() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": {
                \"term\": {
                    \"foo\": \"query\"
                }
            },
            \"filter\": \"foo\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_unexpected_key() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": {
                \"term\": {
                    \"foo\": \"query\"
                }
            },
            \"filter\": {
                \"term\": {
                    \"foo\": \"filter\"
                }
            },
            \"foo\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::UnrecognisedKey("foo".to_string())));
    }
}
