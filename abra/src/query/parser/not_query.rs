//! Parses "not" queries

use rustc_serialize::json::Json;

use query::Query;
use query::parser::{QueryParseContext, QueryParseError, parse as parse_query};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    Ok(Query::Exclude {
        query: Box::new(Query::MatchAll),
        exclude: Box::new(try!(parse_query(&context.clone().no_score(), json))),
    })
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
    fn test_not_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"term\": {
                \"test\":  \"foo\"
            }
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Exclude {
            query: Box::new(Query::MatchAll),
            exclude: Box::new(Query::MatchTerm {
                field: "test".to_string(),
                term: Term::String("foo".to_string()),
                matcher: TermMatcher::Exact,
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
