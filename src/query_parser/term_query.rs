//! Parses "term" queries

use rustc_serialize::json::Json;
use abra::{Term, Query, TermMatcher, TermScorer};
use abra::schema::SchemaRead;

use query_parser::{QueryParseContext, QueryParseError};
use query_parser::utils::parse_float;


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey)
    };

    let object = object.get(field_name).unwrap();

    // Get mapping for field
    let field_mapping = match context.mappings {
        Some(mappings) => mappings.get_field(field_name),
        None => None,
    };

    // Get configuration
    let mut term: Option<Term> = None;
    let mut boost = 1.0f64;

    match *object {
        Json::Object(ref inner_object) => {
            for (key, val) in inner_object.iter() {
                match key.as_ref() {
                    "value" => {
                        term = Term::from_json(val);

                        if term == None {
                            return Err(QueryParseError::InvalidValue);
                        }
                    }
                    "boost" => {
                        boost = try!(parse_float(val));
                    }
                    _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
                }
            }
        }
        _ => term = Term::from_json(object),
    }

    match term {
        Some(term) => {
            let mut query = Query::MatchTerm {
                field: try!(context.schema.get_field_by_name(&field_name).ok_or_else(|| QueryParseError::FieldDoesntExist(field_name.clone()))),
                term: term,
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            };

            // Add boost
            query.boost(boost);

            Ok(query)
        }
        None => Err(QueryParseError::ExpectedKey("value"))
    }
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
    fn test_term_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"value\": \"bar\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchTerm {
            field: foo_field,
            term: Term::String("bar".to_string()),
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default(),
        }));
    }

    #[test]
    fn test_with_number() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"value\": 123
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchTerm {
            field: foo_field,
            term: Term::U64(123),
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default(),
        }));
    }

    #[test]
    fn test_simple_term_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchTerm {
            field: foo_field,
            term: Term::String("bar".to_string()),
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default(),
        }));
    }

    #[test]
    fn test_with_boost() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"value\": \"bar\",
                \"boost\": 2.0
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchTerm {
            field: foo_field,
            term: Term::String("bar".to_string()),
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default_with_boost(2.0f64),
        }));
    }

    #[test]
    fn test_with_boost_integer() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"value\": \"bar\",
                \"boost\": 2
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchTerm {
            field: foo_field,
            term: Term::String("bar".to_string()),
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default_with_boost(2.0f64),
        }));
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
    fn test_gives_error_for_incorrect_boost_type() {
        let (schema, foo_field) = make_one_field_schema();

        // String
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": \"2\"
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": [2]
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
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
    fn test_gives_error_for_unrecognised_field() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"baz\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::FieldDoesntExist("baz".to_string())));
    }

    #[test]
    fn test_gives_error_for_missing_value() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("value")));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
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
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
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
