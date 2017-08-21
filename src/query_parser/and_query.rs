//! Parses "and" queries

use serde_json::Value as Json;
use kite::Query;
use kite::schema::Schema;

use query_parser::{QueryBuildContext, QueryParseError, QueryBuilder, parse as parse_query};


#[derive(Debug)]
struct AndQueryBuilder {
    queries: Vec<Box<QueryBuilder>>,
}


impl QueryBuilder for AndQueryBuilder {
    fn build(&self, context: &QueryBuildContext, schema: &Schema) -> Query {
        let mut queries = Vec::new();

        for query in self.queries.iter() {
            queries.push(query.build(context, schema));
        }

        Query::Conjunction { queries: queries }
    }
}


pub fn parse(json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let filters = try!(json.as_array().ok_or(QueryParseError::ExpectedArray));

    let mut queries = Vec::new();
    for filter in filters.iter() {
        queries.push(try!(parse_query(filter)));
    }

    Ok(Box::new(AndQueryBuilder {
        queries: queries
    }))
}


#[cfg(test)]
mod tests {
    use serde_json;

    use kite::{Term, Query, TermScorer};
    use kite::schema::{Schema, FieldType, FIELD_INDEXED};

    use query_parser::{QueryBuildContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_and_query() {
        let mut schema = Schema::new();
        let test_field = schema.add_field("test".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&serde_json::from_str("
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
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Conjunction {
            queries: vec![
                Query::Term {
                    field: test_field,
                    term: Term::from_string("foo"),
                    scorer: TermScorer::default(),
                },
                Query::Term {
                    field: test_field,
                    term: Term::from_string("bar"),
                    scorer: TermScorer::default(),
                },
            ],
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&serde_json::from_str("
        \"hello\"
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // Object
        let query = parse(&serde_json::from_str("
        {
            \"foo\": \"bar\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // Integer
        let query = parse(&serde_json::from_str("
        123
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // Float
        let query = parse( &serde_json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));
    }
}
