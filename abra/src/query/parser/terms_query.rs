//! Parses "match" queries

use rustc_serialize::json::Json;

use term::Term;
use analysis::Analyzer;
use mapping::FieldMapping;

use query::Query;
use query::term_matcher::TermMatcher;
use query::term_scorer::TermScorer;
use query::parser::{QueryParseContext, QueryParseError};
use query::parser::utils::{parse_string, parse_float, Operator, parse_operator};


pub fn parse(context: &QueryParseContext, json: &Json) -> Result<Query, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey);
    };

    // Get mapping for field
    let field_mapping = match context.mappings {
        Some(mappings) => mappings.get_field(field_name),
        None => None,
    };

    // Get configuration
    let terms = if let Json::Array(ref arr) = object[field_name] {
        arr.clone()
    } else {
        return Err(QueryParseError::ExpectedArray);
    };

    // Create a term query for each token
    let mut sub_queries = Vec::new();
    for term in terms {
        match Term::from_json(&term) {
            Some(term) => {
                sub_queries.push(Query::MatchTerm {
                    field: field_name.clone(),
                    term: term,
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                });
            }
            None => return Err(QueryParseError::InvalidValue)
        }
    }

    Ok(Query::new_disjunction(sub_queries))
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
    fn test_terms_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": [\"bar\", \"baz\"]
        }
        ").unwrap());

        assert_eq!(query, Ok(Query::Disjunction {
            queries: vec![
                Query::MatchTerm {
                    field: "foo".to_string(),
                    term: Term::String("bar".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                },
                Query::MatchTerm {
                    field: "foo".to_string(),
                    term: Term::String("baz".to_string()),
                    matcher: TermMatcher::Exact,
                    scorer: TermScorer::default(),
                }
            ],
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
    fn test_gives_error_for_incorrect_query_type() {
        // Object
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": {
                \"query\": [\"bar\", \"baz\"]
            }
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));

        // String
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": \"bar baz\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedArray));
    }

    #[test]
    fn test_gives_error_for_missing_query() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedSingleKey));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let query = parse(&QueryParseContext::new(), &Json::from_str("
        {
            \"foo\": [\"bar\", \"baz\"],
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::ExpectedSingleKey));
    }
}
