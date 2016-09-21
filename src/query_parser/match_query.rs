//! Parses "match" queries

use rustc_serialize::json::Json;
use abra::{Token, Query, TermMatcher, TermScorer};
use abra::schema::SchemaRead;

use mapping::{FieldSearchOptions};

use query_parser::{QueryParseContext, QueryParseError};
use query_parser::utils::{parse_string, parse_float, Operator, parse_operator};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey)
    };

    // Get search options for field
    let field_search_options = match context.mappings {
        Some(mappings) => {
            match mappings.get_field(field_name) {
                Some(field_mapping) => field_mapping.get_search_options(),
                None => FieldSearchOptions::default(),  // TODO: error?
            }
        }
        None => FieldSearchOptions::default(),  // TODO: error?
    };

    // Get configuration
    let mut query = String::new();
    let mut boost = 1.0f64;
    let mut operator = Operator::Or;

    match object[field_name] {
        Json::String(_) => query = try!(parse_string(&object[field_name])),
        Json::Object(ref inner_object) => {
            let mut has_query_key = false;

            for (key, value) in inner_object.iter() {
                match key.as_ref() {
                    "query" => {
                        has_query_key = true;
                        query = try!(parse_string(value));
                    }
                    "boost" => {
                        boost = try!(parse_float(value));
                    }
                    "operator" => {
                        operator = try!(parse_operator(value))
                    }
                    _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
                }
            }

            if !has_query_key {
                return Err(QueryParseError::ExpectedKey("query"))
            }
        }
        _ => return Err(QueryParseError::ExpectedObjectOrString),
    }

    // Tokenise query string
    let analyzer = field_search_options.analyzer.initialise(&query);
    let tokens = analyzer.collect::<Vec<Token>>();

    // Create a term query for each token
    let mut sub_queries = Vec::new();
    for token in tokens {
        sub_queries.push(Query::MatchTerm {
            field: try!(context.schema.get_field_by_name(&field_name).ok_or_else(|| QueryParseError::FieldDoesntExist(field_name.clone()))),
            term: token.term,
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default(),
        });
    }

    // Combine the term queries
    let mut query = match operator {
        Operator::Or => {
            Query::new_disjunction(sub_queries)
        }
        Operator::And => {
            Query::new_conjunction(sub_queries)
        }
    };

    // Add boost
    query.boost(boost);

    return Ok(query);
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
    fn test_match_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchTerm {
            field: foo_field,
            term: Term::String("bar".to_string()),
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default(),
        }))
    }

    #[test]
    fn test_multi_term_match_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar baz\"
            }
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
    fn test_simple_multi_term_match_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": \"bar baz\"
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
    fn test_with_boost() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": 2.0
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchTerm {
            field: foo_field,
            term: Term::String("bar".to_string()),
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default_with_boost(2.0f64),
        }))
    }

    #[test]
    fn test_with_boost_integer() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": 2
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchTerm {
            field: foo_field,
            term: Term::String("bar".to_string()),
            matcher: TermMatcher::Exact,
            scorer: TermScorer::default_with_boost(2.0f64),
        }))
    }

    #[test]
    fn test_with_and_operator() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar baz\",
                \"operator\": \"and\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Conjunction {
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
    fn test_gives_error_for_missing_query() {
        let (schema, foo_field) = make_one_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"foo\": {
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("query")));
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
