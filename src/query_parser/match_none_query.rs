//! Parses "match_none" queries

use rustc_serialize::json::Json;
use kite::Query;

use query_parser::{QueryParseContext, QueryParseError, QueryBuilder};


#[derive(Debug)]
struct MatchNoneQueryBuilder;


impl QueryBuilder for MatchNoneQueryBuilder {
    fn build(&self) -> Query {
        Query::MatchNone
    }
}


pub fn parse(_context: &QueryParseContext, json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

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
    use rustc_serialize::json::Json;

    use kite::{Term, Query};

    use query_parser::{QueryParseContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_match_none_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
        }
        ").unwrap()).and_then(|builder| Ok(builder.build()));

        assert_eq!(query, Ok(Query::MatchNone))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
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
    fn test_gives_error_for_unrecognised_key() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
