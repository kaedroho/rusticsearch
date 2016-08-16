//! Parses "match_none" queries

use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Get configuration
    for (key, value) in object.iter() {
        match &key[..] {
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }

    Ok(Query::MatchNone)
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use term::Term;
    use query::Query;
    use query::term_matcher::TermMatcher;
    use query::parser::{QueryParseContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_match_none_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
        }
        ").unwrap());

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

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        123
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_unrecognised_key() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
