//! Parses "match" queries

use rustc_serialize::json::Json;
use kite::{Term, Token, Query, TermScorer};
use kite::schema::Schema;

use mapping::FieldSearchOptions;

use query_parser::{QueryBuildContext, QueryParseError, QueryBuilder};
use query_parser::utils::{parse_string, parse_float, Operator, parse_operator};


#[derive(Debug)]
struct MatchQueryBuilder {
    field: String,
    query: String,
    operator: Operator,
    boost: f64,
}


impl QueryBuilder for MatchQueryBuilder {
    fn build(&self, context: &QueryBuildContext, schema: &Schema) -> Query {
        // Get search options for field
        let field_search_options = match context.mappings {
            Some(mappings) => {
                match mappings.get_field(&self.field) {
                    Some(field_mapping) => field_mapping.get_search_options(),
                    None => FieldSearchOptions::default(),  // TODO: error?
                }
            }
            None => FieldSearchOptions::default(),  // TODO: error?
        };

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

        // Create a term query for each token
        let mut sub_queries = Vec::new();
        for token in tokens {
            sub_queries.push(Query::Term {
                field: schema.get_field_by_name(&self.field).unwrap(),
                term: token.term,
                scorer: TermScorer::default(),
            });
        }

        // Combine the term queries
        let mut query = match self.operator {
            Operator::Or => {
                Query::new_disjunction(sub_queries)
            }
            Operator::And => {
                Query::new_conjunction(sub_queries)
            }
        };

        // Add boost
        query.boost(self.boost);

        query
    }
}


pub fn parse(json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey)
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

    Ok(Box::new(MatchQueryBuilder {
        field: field_name.clone(),
        query: query,
        operator: operator,
        boost: boost,
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
    fn test_match_query() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\"
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Term {
            field: foo_field,
            term: Term::String("bar".to_string()),
            scorer: TermScorer::default(),
        }))
    }

    #[test]
    fn test_multi_term_match_query() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar baz\"
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Disjunction {
            queries: vec![
                Query::Term {
                    field: foo_field,
                    term: Term::String("bar".to_string()),
                    scorer: TermScorer::default(),
                },
                Query::Term {
                    field: foo_field,
                    term: Term::String("baz".to_string()),
                    scorer: TermScorer::default(),
                }
            ],
        }))
    }

    #[test]
    fn test_simple_multi_term_match_query() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": \"bar baz\"
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Disjunction {
            queries: vec![
                Query::Term {
                    field: foo_field,
                    term: Term::String("bar".to_string()),
                    scorer: TermScorer::default(),
                },
                Query::Term {
                    field: foo_field,
                    term: Term::String("baz".to_string()),
                    scorer: TermScorer::default(),
                }
            ],
        }))
    }

    #[test]
    fn test_with_boost() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": 2.0
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Term {
            field: foo_field,
            term: Term::String("bar".to_string()),
            scorer: TermScorer::default_with_boost(2.0f64),
        }))
    }

    #[test]
    fn test_with_boost_integer() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": 2
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Term {
            field: foo_field,
            term: Term::String("bar".to_string()),
            scorer: TermScorer::default_with_boost(2.0f64),
        }))
    }

    #[test]
    fn test_with_and_operator() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar baz\",
                \"operator\": \"and\"
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Conjunction {
            queries: vec![
                Query::Term {
                    field: foo_field,
                    term: Term::String("bar".to_string()),
                    scorer: TermScorer::default(),
                },
                Query::Term {
                    field: foo_field,
                    term: Term::String("baz".to_string()),
                    scorer: TermScorer::default(),
                }
            ],
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
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
    fn test_gives_error_for_incorrect_boost_type() {
        // String
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": \"2\"
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": [2]
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": {
                    \"value\": 2
                }
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));
    }

    #[test]
    fn test_gives_error_for_missing_query() {
        let query = parse(&Json::from_str("
        {
            \"foo\": {
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedKey("query")));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\"
            },
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedSingleKey));
    }

    #[test]
    fn test_gives_error_for_extra_inner_key() {
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"hello\": \"world\"
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
