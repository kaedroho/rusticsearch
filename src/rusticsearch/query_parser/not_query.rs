//! Parses "not" queries

use rustc_serialize::json::Json;
use kite::Query;
use kite::schema::Schema;

use query_parser::{QueryParseContext, QueryParseError, QueryBuilder, parse as parse_query};


#[derive(Debug)]
struct NotQueryBuilder {
    query: Box<QueryBuilder>,
}


impl QueryBuilder for NotQueryBuilder {
    fn build(&self, schema: &Schema) -> Query {
        Query::Exclude {
            query: Box::new(Query::new_match_all()),
            exclude: Box::new(self.query.build(schema)),
        }
    }
}


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    Ok(Box::new(NotQueryBuilder {
        query: try!(parse_query(&context.clone().no_score(), json)),
    }))
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use kite::{Term, Query, TermScorer};
    use kite::schema::{Schema, FieldType, FIELD_INDEXED};

    use query_parser::{QueryParseContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_not_query() {
        let mut schema = Schema::new();
        let test_field = schema.add_field("test".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"term\": {
                \"test\":  \"foo\"
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&schema)));

        assert_eq!(query, Ok(Query::Exclude {
            query: Box::new(Query::new_match_all()),
            exclude: Box::new(Query::MatchTerm {
                field: test_field,
                term: Term::String("foo".to_string()),
                scorer: TermScorer::default(),
            }),
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        \"hello\"
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
}
