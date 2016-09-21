//! Parses "not" queries

use rustc_serialize::json::Json;
use abra::Query;

use query_parser::{QueryParseContext, QueryParseError, parse as parse_query};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    Ok(Query::Exclude {
        query: Box::new(Query::new_match_all()),
        exclude: Box::new(try!(parse_query(&context.clone().no_score(), json))),
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
    fn test_not_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"term\": {
                \"foo\":  \"test\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Exclude {
            query: Box::new(Query::new_match_all()),
            exclude: Box::new(Query::MatchTerm {
                field: foo_field,
                term: Term::String("test".to_string()),
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
}
