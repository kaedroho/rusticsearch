//! Parses "multi_match" queries

use rustc_serialize::json::Json;
use kite::{Term, Token, Query, TermScorer};

use mapping::FieldSearchOptions;

use query_parser::{QueryParseContext, QueryParseError, QueryBuilder};
use query_parser::utils::{parse_string, parse_float, Operator, parse_operator, parse_field_and_boost};


#[derive(Debug)]
struct MultiMatchQueryBuilder {
    fields: Vec<(String, f64, FieldSearchOptions)>,
    query: String,
    operator: Operator,
    boost: f64,
}


impl QueryBuilder for MultiMatchQueryBuilder {
    fn build(&self) -> Query {
        // Convert query string into term query objects
        let mut field_queries = Vec::new();
        for &(ref field_name, field_boost, ref field_search_options) in self.fields.iter() {
            // Tokenise query string
            let tokens = match field_search_options.analyzer {
                Some(ref analyzer) => {
                    let token_stream = analyzer.initialise(&self.query);
                    token_stream.collect::<Vec<Token>>()
                }
                None => {
                    vec![Token {term: Term::String(self.query.clone()), position: 1}]
                }
            };

            let mut term_queries = Vec::new();
            for token in tokens {
                term_queries.push(Query::MatchTerm {
                    field: field_name.clone(),
                    term: token.term,
                    scorer: TermScorer::default(),
                });
            }

            let mut field_query = match self.operator {
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
        query.boost(self.boost);

        query
    }
}


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
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

    // Add search options to fields
    let mut fields = Vec::new();
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

        fields.push((field_name, field_boost, field_search_options));
    }

    Ok(Box::new(MultiMatchQueryBuilder {
        fields: fields,
        query: query,
        operator: operator,
        boost: boost,
    }))
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use kite::{Term, Query, TermScorer};

    use query_parser::{QueryParseContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_multi_match_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: "bar".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default(),
                },
                Query::MatchTerm {
                    field: "baz".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default(),
                }
            ],
        }));
    }

    #[test]
    fn test_multi_term_multi_match_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"hello world\",
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Disjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: "bar".to_string(),
                            term: Term::String("hello".to_string()),
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: "bar".to_string(),
                            term: Term::String("world".to_string()),
                            scorer: TermScorer::default(),
                        }
                    ],
                },
                Query::Disjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("hello".to_string()),
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("world".to_string()),
                            scorer: TermScorer::default(),
                        }
                    ],
                }
            ],
        }));
    }

    #[test]
    fn test_with_boost() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": 2.0
        }
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: "bar".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: "baz".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                }
            ],
        }));
    }

    #[test]
    fn test_with_boost_integer() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": 2
        }
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: "bar".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: "baz".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                }
            ],
        }));
    }

    #[test]
    fn test_with_field_boost() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar^2\", \"baz^1.0\"]
        }
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: "bar".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: "baz".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default(),
                }
            ],
        }));
    }

    #[test]
    fn test_with_field_and_query_boost() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar^2\", \"baz^1.0\"],
            \"boost\": 2.0
        }
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::MatchTerm {
                    field: "bar".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default_with_boost(4.0f64),
                },
                Query::MatchTerm {
                    field: "baz".to_string(),
                    term: Term::String("foo".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                }
            ],
        }));
    }

    #[test]
    fn test_with_and_operator() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo bar\",
            \"fields\": [\"baz\", \"quux\"],
            \"operator\": \"and\"
        }
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::DisjunctionMax {
            queries: vec![
                Query::Conjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("foo".to_string()),
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: "baz".to_string(),
                            term: Term::String("bar".to_string()),
                            scorer: TermScorer::default(),
                        }
                    ],
                },
                Query::Conjunction {
                    queries: vec![
                        Query::MatchTerm {
                            field: "quux".to_string(),
                            term: Term::String("foo".to_string()),
                            scorer: TermScorer::default(),
                        },
                        Query::MatchTerm {
                            field: "quux".to_string(),
                            term: Term::String("bar".to_string()),
                            scorer: TermScorer::default(),
                        }
                    ],
                }
            ],
        }));
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        \"foo\"
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
    fn test_gives_error_for_incorrect_query_type() {
        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": {
                \"foo\": \"bar\"
            },
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedString));

        // Array
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": [\"foo\"],
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedString));

        // Integer
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": 123,
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedString));

        // Float
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": 123.456,
            \"fields\": [\"bar\", \"baz\"]
        }        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedString));
    }

    #[test]
    fn test_gives_error_for_incorrect_fields_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": {
                \"value\": [\"bar\", \"baz\"]
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // Integer
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": 123
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // Float
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": 123.456
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));
    }

    #[test]
    fn test_gives_error_for_incorrect_boost_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": \"2\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": [2]
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"boost\": {
                \"value\": 2
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));
    }

    #[test]
    fn test_gives_error_for_missing_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"fields\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedKey("query")));
    }

    #[test]
    fn test_gives_error_for_missing_fields() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedKey("fields")));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"query\": \"foo\",
            \"fields\": [\"bar\", \"baz\"],
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
