use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError, parse as parse_query};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    Ok(Query::Filter {
        query: Box::new(Query::MatchAll),
        filter: Box::new(try!(parse_query(context, json)).to_boolean_query().negate()),
    })
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use term::Term;
    use query::{Query, TermMatcher};
    use query::boolean::BooleanQuery;
    use query::parser::{QueryParseContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_not_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"term\": {
                \"test\":  \"foo\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Filter {
            query: Box::new(Query::MatchAll),
            filter: Box::new(BooleanQuery::NotMatchTerm {
                field: "test".to_string(),
                term: Term::String("foo".to_string()),
                matcher: TermMatcher::Exact
            }),
        }))
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        \"hello\"
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedObject));

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
}
