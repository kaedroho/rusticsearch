//! Parses "match" queries

use rustc_serialize::json::Json;
use abra::{Term, Query, TermMatcher, TermScorer};

use query_parser::{QueryParseContext, QueryParseError};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey);
    };

    // Get mapping for field
    let field_mapping = match context.mappings {
        Some(mappings) => mappings.get_field(field_name),
        None => None,
    };

    // Get configuration
    let terms = if let Json::Array(ref arr) = object[field_name] {
        arr.clone()
    } else {
        return Err(QueryParseError::ExpectedArray);
    };

    // Create a term query for each token
    let mut sub_queries = Vec::new();
    for term in terms {
        match Term::from_json(&term) {
            Some(term) => {
                sub_queries.push(Query::MatchTerm {
                    field: try!(context.schema.get_field_by_name(&field_name).ok_or_else(|| QueryParseError::FieldDoesntExist(field_name.clone()))),
                    term: term,
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                });
            }
            None => return Err(QueryParseError::InvalidValue)
        }
    }

    Ok(Query::new_disjunction(sub_queries))
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use abra::{Term, Query, TermMatcher, TermScorer};
    use abra::schema::{Schema, FieldType, FieldRef};

    use query_parser::{QueryParseContext, QueryParseError};
    use mapping::{MappingRegistry, Mapping, FieldMapping};

    use super::parse;

    fn make_one_field_schema() -> (Schema, FieldRef) {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text).unwrap();
        (schema, foo_field)
    }

    #[test]
    fn test_terms_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Disjunction {
            queries: vec![
                Query::MatchTerm {
                    field: foo_field,
                    term: Term::String("bar".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                },
                Query::MatchTerm {
                    field: foo_field,
                    term: Term::String("baz".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                }
            ],
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        let (schema, foo_field) = make_one_field_schema();

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
    fn test_gives_error_for_incorrect_query_type() {
        let (schema, foo_field) = make_one_field_schema();

        // Object
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"query\": [\"bar\", \"baz\"]
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // String
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": \"bar baz\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));
    }

    #[test]
    fn test_gives_error_for_unrecognised_field() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"baz\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::FieldDoesntExist("baz".to_string())));
    }

    #[test]
    fn test_gives_error_for_missing_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedSingleKey));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": [\"bar\", \"baz\"],
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedSingleKey));
    }
}
