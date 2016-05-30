use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError, parse as parse_query};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let sub_query = try!(parse_query(context, json));

    Ok(Query::Bool {
        must: vec![
            Query::MatchAll
        ],
        must_not: vec![sub_query],
        should: vec![],
        filter: vec![],
        minimum_should_match: 0,
        boost: 1.0f64,
    })
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use term::Term;
    use query::{Query, TermMatcher};
    use query::parser::{QueryParseContext, QueryParseError};
    use index::Index;

    use super::parse;

    #[test]
    fn test_not_query() {
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        {
            \"term\": {
                \"test\":  \"foo\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Bool {
            must: vec![
                Query::MatchAll
            ],
            must_not: vec![
                Query::MatchTerm {
                    field: "test".to_string(),
                    term: Term::String("foo".to_string()),
                    boost: 1.0f64,
                    matcher: TermMatcher::Exact
                }
            ],
            should: vec![],
            filter: vec![],
            minimum_should_match: 0,
            boost: 1.0f64,
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        \"hello\"
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Array
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        [
            \"foo\"
        ]
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        123
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&QueryParseContext::new(&Index::new()), &Json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));
    }
}
