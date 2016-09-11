//! Parses "multi_match" queries

use rustc_serialize::json::Json;
use abra::{Token, Query, TermMatcher, TermScorer};

use mapping::FieldSearchOptions;

use query_parser::{QueryParseContext, QueryParseError};
use query_parser::utils::{parse_string, parse_float, Operator, parse_operator, parse_field_and_boost};


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
        // Get search options for field
        let field_search_options = match context.mappings {
            Some(mappings) => {
                match mappings.get_field(&field_name) {
                    Some(field_mapping) => field_mapping.get_search_options(),
                    None => FieldSearchOptions::default(),  // TODO: error?
                }
            }
            None => FieldSearchOptions::default(),  // TODO: error?
        };

        // Tokenise query string
        let analyzer = field_search_options.analyzer.initialise(&query);
        let tokens = analyzer.collect::<Vec<Token>>();

        let mut term_queries = Vec::new();
        for token in tokens {
            term_queries.push(Query::MatchTerm {
                field: context.schema.get_field_by_name(&field_name).unwrap(), // TODO: Error if field doesn't exist
                term: token.term,
                matcher: TermMatcher::Exact,
                scorer: TermScorer::default(),
            });
        }

        let mut field_query = match operator {
            Operator::Or => {
                Query::new_disjunction(term_queries)
            }
            Operator::And => {
                Query::new_conjunction(term_queries)
            }
        };

        // Add boost
        field_query.boost(field_boost);

        field_queries.push(field_query);
    }

    let mut query = Query::new_disjunction_max(field_queries);

    // Add boost
    query.boost(boost);

    return Ok(query);
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use abra::{Term, Query, TermMatcher, TermScorer};
    use abra::schema::{Schema, FieldType, FieldRef};

    use query_parser::{QueryParseContext, QueryParseError};
    use mapping::{MappingRegistry, Mapping, FieldMapping};

    use super::parse;

    fn make_two_field_schema() -> (Schema, FieldRef, FieldRef) {
        let mut schema = Schema::new();
        let bar_field = schema.add_field("bar".to_string(), FieldType::Text).unwrap();
        let baz_field = schema.add_field("baz".to_string(), FieldType::Text).unwrap();
        (schema, bar_field, baz_field)
    }

    #[test]
    fn test_multi_match_query() {
        let (schema, bar_field, baz_field) = make_two_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: bar_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                },
                Query::MatchTerm {
                    field: baz_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                }
            ],
        }));
    }

    #[test]
    fn test_multi_term_multi_match_query() {
        let (schema, bar_field, baz_field) = make_two_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"hello world\",
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Disjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: bar_field,
                            term: Term::String("hello".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: bar_field,
                            term: Term::String("world".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        }
                    ],
                },
                Query::Disjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: baz_field,
                            term: Term::String("hello".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: baz_field,
                            term: Term::String("world".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        }
                    ],
                }
            ],
        }));
    }

    #[test]
    fn test_with_boost() {
        let (schema, bar_field, baz_field) = make_two_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": 2.0
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: bar_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: baz_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default_with_boost(2.0f64),
                }
            ],
        }));
    }

    #[test]
    fn test_with_boost_integer() {
        let (schema, bar_field, baz_field) = make_two_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": 2
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: bar_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: baz_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default_with_boost(2.0f64),
                }
            ],
        }));
    }

    #[test]
    fn test_with_field_boost() {
        let (schema, bar_field, baz_field) = make_two_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar^2\", \"baz^1.0\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: bar_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: baz_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                }
            ],
        }));
    }

    #[test]
    fn test_with_field_and_query_boost() {
        let (schema, bar_field, baz_field) = make_two_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar^2\", \"baz^1.0\"],
            \"boost\": 2.0
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: bar_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default_with_boost(4.0f64),
                },
                Query::MatchTerm {
                    field: baz_field,
                    term: Term::String("foo".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default_with_boost(2.0f64),
                }
            ],
        }));
    }

    #[test]
    fn test_with_and_operator() {
        let (schema, bar_field, baz_field) = make_two_field_schema();
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo quux\",
            \"fields\": [\"bar\", \"baz\"],
            \"operator\": \"and\"
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Conjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: bar_field,
                            term: Term::String("foo".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: bar_field,
                            term: Term::String("quux".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        }
                    ],
                },
                Query::Conjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: baz_field,
                            term: Term::String("foo".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: baz_field,
                            term: Term::String("quux".to_string()),
                            matcher: TermMatcher::Exact,
                            scorer: TermScorer::default(),
                        }
                    ],
                }
            ],
        }));
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        let (schema, bar_field, baz_field) = make_two_field_schema();

        // String
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        \"foo\"
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
    fn test_gives_error_for_incorrect_query_type() {
        let (schema, bar_field, baz_field) = make_two_field_schema();

        // Object
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": {
                \"foo\": \"bar\"
            },
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));

        // Array
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": [\"foo\"],
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));

        // Integer
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": 123,
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));

        // Float
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": 123.456,
            \"fields\": [\"bar\", \"baz\"]
        }        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedString));
    }

    #[test]
    fn test_gives_error_for_incorrect_fields_type() {
        let (schema, bar_field, baz_field) = make_two_field_schema();

        // String
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // Object
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": {
                \"value\": [\"bar\", \"baz\"]
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // Integer
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": 123
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // Float
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": 123.456
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));
    }

    #[test]
    fn test_gives_error_for_incorrect_boost_type() {
        let (schema, bar_field, baz_field) = make_two_field_schema();

        // String
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": \"2\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": [2]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
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
        let (schema, bar_field, baz_field) = make_two_field_schema();

        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("query")));
    }

    #[test]
    fn test_gives_error_for_missing_fields() {
        let (schema, bar_field, baz_field) = make_two_field_schema();

        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedKey("fields")));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let (schema, bar_field, baz_field) = make_two_field_schema();

        let query = parse(&QueryParseContext::new(&schema), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
