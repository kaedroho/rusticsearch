//! Parses "match_all" queries

use serde_json::Value as Json;
use kite::Query;
use kite::schema::Schema;

use query_parser::{QueryBuildContext, QueryParseError, QueryBuilder};
use query_parser::utils::parse_float;


#[derive(Debug)]
struct MatchAllQueryBuilder {
    boost: f32,
}


impl QueryBuilder for MatchAllQueryBuilder {
    fn build(&self, _context: &QueryBuildContext, _schema: &Schema) -> Query {
        Query::all().boost(self.boost)
    }
}


pub fn parse(json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let object = json.as_object().ok_or(QueryParseError::ExpectedObject)?;

    // Get configuration
    let mut boost = 1.0f32;

    for (key, value) in object.iter() {
        match &key[..] {
            "boost" => {
                boost = parse_float(value)?;
            }
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    Ok(Box::new(MatchAllQueryBuilder {
        boost: boost,
    }))
}


#[cfg(test)]
mod tests {
    use serde_json;

    use kite::Query;
    use kite::schema::Schema;

    use query_parser::{QueryBuildContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_match_all_query() {
        let schema = Schema::new();

        let query = parse(&serde_json::from_str("
        {
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::All {score: 1.0f32}))
    }

    #[test]
    fn test_with_boost() {
        let schema = Schema::new();

        let query = parse( &serde_json::from_str("
        {
            \"boost\": 2.0
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::All {score: 2.0f32}))
    }

    #[test]
    fn test_with_boost_integer() {
        let schema = Schema::new();

        let query = parse(&serde_json::from_str("
        {
            \"boost\": 2
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::All {score: 2.0f32}))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
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

    #[test]
    fn test_gives_error_for_incorrect_boost_type() {
        // String
        let query = parse(&serde_json::from_str("
        {
            \"boost\": \"2\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&serde_json::from_str("
        {
            \"boost\": [2]
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&serde_json::from_str("
        {
            \"boost\": {
                \"value\": 2
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));
    }

    #[test]
    fn test_gives_error_for_unrecognised_key() {
        let query = parse(&serde_json::from_str("
        {
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
