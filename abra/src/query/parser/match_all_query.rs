//! Parses "match_all" queries

use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::utils::parse_float;


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Get configuration
    let mut boost = 1.0f64;

    for (key, value) in object.iter() {
        match &key[..] {
            "boost" => {
                boost = try!(parse_float(value));
            }
            _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
        }
    }


    let mut query = Query::MatchAll;

    // Add boost
    query = Query::new_boost(query, boost);

    return Ok(query);
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use term::Term;
    use query::Query;
    use query::term_matcher::TermMatcher;
    use query::term_scorer::TermScorer;
    use query::parser::{QueryParseContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_match_all_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchAll))
    }

    #[test]
    fn test_with_boost() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"boost\": 2.0
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Boost {
            query: Box::new(Query::MatchAll),
            boost: 2.0f64,
        }))
    }

    #[test]
    fn test_with_boost_integer() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"boost\": 2
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Boost {
            query: Box::new(Query::MatchAll),
            boost: 2.0f64,
        }))
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
    fn test_gives_error_for_incorrect_boost_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"boost\": \"2\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"boost\": [2]
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"boost\": {
                \"value\": 2
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedFloat));
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
