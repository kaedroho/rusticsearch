//! Parses "not" queries

use serde_json::Value as Json;
use search::Query;
use search::schema::Schema;

use query_parser::{QueryBuildContext, QueryParseError, QueryBuilder, parse as parse_query};


#[derive(Debug)]
struct NotQueryBuilder {
    query: Box<QueryBuilder>,
}


impl QueryBuilder for NotQueryBuilder {
    fn build(&self, context: &QueryBuildContext, schema: &Schema) -> Query {
        Query::Exclude {
            query: Box::new(Query::all()),
            exclude: Box::new(self.query.build(&context.clone().no_score(), schema)),
        }
    }
}


pub fn parse(json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    Ok(Box::new(NotQueryBuilder {
        query: parse_query(json)?,
    }))
}


#[cfg(test)]
mod tests {
    use serde_json;

    use search::{Term, Query, TermScorer};
    use search::schema::{Schema, FieldType, FIELD_INDEXED};

    use query_parser::{QueryBuildContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_not_query() {
        let mut schema = Schema::new();
        let test_field = schema.add_field("test".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&&serde_json::from_str("
        {
            \"term\": {
                \"test\":  \"foo\"
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Exclude {
            query: Box::new(Query::all()),
            exclude: Box::new(Query::Term {
                field: test_field,
                term: Term::from_string("foo"),
                scorer: TermScorer::default(),
            }),
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&serde_json::from_str("
        \"hello\"
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Array
        let query = parse(&serde_json::from_str("
        [
            \"foo\"
        ]
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&serde_json::from_str("
        123
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&serde_json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));
    }
}
