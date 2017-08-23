//! Parses "constant_score" queries

use serde_json::Value as Json;
use kite::Query;
use kite::schema::Schema;

use query_parser::{QueryBuildContext, QueryParseError, QueryBuilder, parse as parse_query};
use query_parser::utils::parse_float;

#[derive(Debug)]
struct ConstantScoreQueryBuilder {
    filter: Box<QueryBuilder>,
    score: f32,
}

impl QueryBuilder for ConstantScoreQueryBuilder {
    fn build(&self, context: &QueryBuildContext, schema: &Schema) -> Query {
        Query::Filter {
            query: Box::new(Query::All{ score: self.score }),
            filter: Box::new(self.filter.build(&context.clone().no_score(), schema)),
        }
    }
}

pub fn parse(json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let object = json.as_object().ok_or(QueryParseError::ExpectedObject)?;

    let filter = match object.get("filter") {
        Some(inner) => parse_query(inner)?,
        None => return Err(QueryParseError::ExpectedKey("filter")),
    };

    let boost = match object.get("boost") {
        Some(inner) => parse_float(inner)?,
        None => return Err(QueryParseError::ExpectedKey("boost")),
    };

    // Check for any keys that we don't recognise
    for key in object.keys() {
        match key.as_ref() {
            "filter" | "boost" => {},
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    Ok(Box::new(ConstantScoreQueryBuilder {
        filter: filter,
        score: boost,
    }))
}

#[cfg(test)]
mod tests {
    use kite::{Term, Query, TermScorer};
    use kite::schema::{Schema, FieldType, FIELD_INDEXED};

    use query_parser::{QueryBuildContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_constant_score_query() {
        let mut schema = Schema::new();
        let test_field = schema.add_field("test".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&json!({
            "filter": {
                "term": {
                    "test": "foo"
                },
            },
            "boost": 2.0
        })).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::All { score: 2.0 }),
            filter: Box::new(Query::Term {
                field: test_field,
                term: Term::from_string("foo"),
                scorer: TermScorer::default(),
            })
        }))
    }

    #[test]
    fn test_missing_filter() {
        let mut schema = Schema::new();
        schema.add_field("test".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&json!({
            "boost": 2.0
        })).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Err(QueryParseError::ExpectedKey("filter")));
    }

    #[test]
    fn test_missing_boost() {
        let mut schema = Schema::new();
        schema.add_field("test".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&json!({
            "filter": {
                "term": {
                    "test": "foo"
                },
            }
        })).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Err(QueryParseError::ExpectedKey("boost")));
    }

    #[test]
    fn test_extra_key() {
        let mut schema = Schema::new();
        schema.add_field("test".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&json!({
            "filter": {
                "term": {
                    "test": "foo"
                },
            },
            "boost": 2.0,
            "foo": "bar"
        })).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Err(QueryParseError::UnrecognisedKey("foo".to_string())));
    }

    #[test]
    fn test_invalid_query() {
        let mut schema = Schema::new();
        schema.add_field("test".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&json!({
            "filter": "foo",
            "boost": 2.0
        })).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

       assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&json!("hello"));
        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Array
        let query = parse(&json!(["hello"]));
        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&json!(123));

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&json!(123.456));
        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));
    }
}
