//! Parses "match" queries

use serde_json::Value as Json;
use kite::{Term, Query, TermScorer};
use kite::schema::Schema;

use query_parser::{QueryBuildContext, QueryParseError, QueryBuilder};
use query_parser::utils::json_value_to_term;

#[derive(Debug)]
struct TermsQueryBuilder {
    field: String,
    terms: Vec<Term>,
}


impl QueryBuilder for TermsQueryBuilder {
    fn build(&self, _context: &QueryBuildContext, schema: &Schema) -> Query {
        // Create a term query for each token
        let mut queries = Vec::new();
        for term in self.terms.iter() {
            queries.push(Query::Term {
                field: schema.get_field_by_name(&self.field).unwrap(),
                term: term.clone(),
                scorer: TermScorer::default(),
            });
        }

        Query::Disjunction { queries: queries }
    }
}


pub fn parse(json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let object = json.as_object().ok_or(QueryParseError::ExpectedObject)?;

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey);
    };

    // Get configuration
    let terms: Vec<Term> = if let &Json::Array(ref arr) = object.get(field_name).unwrap() {
        arr.iter().filter_map(|term| json_value_to_term(&term)).collect()
    } else {
        return Err(QueryParseError::ExpectedArray);
    };

    Ok(Box::new(TermsQueryBuilder {
        field: field_name.clone(),
        terms: terms,
    }))
}


#[cfg(test)]
mod tests {
    use serde_json;

    use kite::{Term, Query, TermScorer};

    use query_parser::{QueryBuildContext, QueryParseError};
    use kite::schema::{Schema, FieldType, FIELD_INDEXED};

    use super::parse;

    #[test]
    fn test_terms_query() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&serde_json::from_str("
        {
            \"foo\": [\"bar\", \"baz\"]
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::Disjunction {
            queries: vec![
                Query::Term {
                    field: foo_field,
                    term: Term::from_string("bar"),
                    scorer: TermScorer::default(),
                },
                Query::Term {
                    field: foo_field,
                    term: Term::from_string("baz"),
                    scorer: TermScorer::default(),
                }
            ],
        }))
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
    fn test_gives_error_for_incorrect_query_type() {
        // Object
        let query = parse(&serde_json::from_str("
        {
            \"foo\": {
                \"query\": [\"bar\", \"baz\"]
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));

        // String
        let query = parse(&serde_json::from_str("
        {
            \"foo\": \"bar baz\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedArray));
    }

    #[test]
    fn test_gives_error_for_missing_query() {
        let query = parse(&serde_json::from_str("
        {
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedSingleKey));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let query = parse(&serde_json::from_str("
        {
            \"foo\": [\"bar\", \"baz\"],
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedSingleKey));
    }
}
