//! Parses "match_none" queries

use serde_json::Value as Json;
use search::Query;
use search::schema::Schema;

use query_parser::{QueryBuildContext, QueryParseError, QueryBuilder};


#[derive(Debug)]
struct MatchNoneQueryBuilder;


impl QueryBuilder for MatchNoneQueryBuilder {
    fn build(&self, _context: &QueryBuildContext, _schema: &Schema) -> Query {
        Query::None
    }
}


pub fn parse(json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let object = json.as_object().ok_or(QueryParseError::ExpectedObject)?;

    // Get configuration
    for (key, _value) in object.iter() {
        match &key[..] {
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    Ok(Box::new(MatchNoneQueryBuilder))
}


#[cfg(test)]
mod tests {
    use serde_json;

    use search::Query;
    use search::schema::Schema;

    use query_parser::{QueryBuildContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_match_none_query() {
        let schema = Schema::new();

        let query = parse(&serde_json::from_str("
        {
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::None))
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
    fn test_gives_error_for_unrecognised_key() {
        let query = parse(&serde_json::from_str("
        {
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
