//! Parses "prefix" queries

use rustc_serialize::json::Json;
use kite::{Query, TermSelector, TermScorer};
use kite::schema::Schema;

use query_parser::{QueryBuildContext, QueryParseError, QueryBuilder};
use query_parser::utils::parse_float;


#[derive(Debug)]
struct PrefixQueryBuilder {
    field: String,
    prefix: String,
    boost: f64,
}


impl QueryBuilder for PrefixQueryBuilder {
    fn build(&self, _context: &QueryBuildContext, schema: &Schema) -> Query {
        let mut query = Query::MatchMultiTerm {
            field: schema.get_field_by_name(&self.field).unwrap(),
            term_selector: TermSelector::Prefix(self.prefix.clone()),
            scorer: TermScorer::default(),
        };

        // Add boost
        query.boost(self.boost);

        query
    }
}


pub fn parse(json: &Json) -> Result<Box<QueryBuilder>, QueryParseError> {
    let object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let field_name = if object.len() == 1 {
        object.keys().collect::<Vec<_>>()[0]
    } else {
        return Err(QueryParseError::ExpectedSingleKey)
    };

    let object = object.get(field_name).unwrap();

    // Get configuration
    let mut value: Option<&Json> = None;
    let mut boost = 1.0f64;

    match *object {
        Json::String(_) => value = Some(object),
        Json::Object(ref inner_object) => {
            for (key, val) in inner_object.iter() {
                match key.as_ref() {
                    "value" => {
                        value = Some(val);
                    }
                    "prefix" => {
                        value = Some(val);
                    }
                    "boost" => {
                        boost = try!(parse_float(val));
                    }
                    _ => return Err(QueryParseError::UnrecognisedKey(key.clone()))
                }
            }
        }
        _ => return Err(QueryParseError::ExpectedObjectOrString),
    }

    match value {
        Some(value) => {
            if let Json::String(ref string) = *value {
                Ok(Box::new(PrefixQueryBuilder {
                    field: field_name.clone(),
                    prefix: string.clone(),
                    boost: boost,
                }))
            } else {
                Err(QueryParseError::ExpectedString)
            }
        }
        None => Err(QueryParseError::ExpectedKey("value"))
    }
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use kite::{Term, Query, TermSelector, TermScorer};
    use kite::schema::{Schema, FieldType, FIELD_INDEXED};

    use query_parser::{QueryBuildContext, QueryParseError};

    use super::parse;

    #[test]
    fn test_prefix_query() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"value\": \"bar\"
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::MatchMultiTerm {
            field: foo_field,
            term_selector: TermSelector::Prefix("bar".to_string()),
            scorer: TermScorer::default(),
        }));
    }

    #[test]
    fn test_simple_prefix_query() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": \"bar\"
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::MatchMultiTerm {
            field: foo_field,
            term_selector: TermSelector::Prefix("bar".to_string()),
            scorer: TermScorer::default(),
        }));
    }

    #[test]
    fn test_with_prefix_key() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"prefix\": \"bar\"
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::MatchMultiTerm {
            field: foo_field,
            term_selector: TermSelector::Prefix("bar".to_string()),
            scorer: TermScorer::default(),
        }));
    }

    #[test]
    fn test_with_boost() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"value\": \"bar\",
                \"boost\": 2.0
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::MatchMultiTerm {
            field: foo_field,
            term_selector: TermSelector::Prefix("bar".to_string()),
            scorer: TermScorer::default_with_boost(2.0f64),
        }));
    }

    #[test]
    fn test_with_boost_integer() {
        let mut schema = Schema::new();
        let foo_field = schema.add_field("foo".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();

        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"value\": \"bar\",
                \"boost\": 2
            }
        }
        ").unwrap()).and_then(|builder| Ok(builder.build(&QueryBuildContext::new(), &schema)));

        assert_eq!(query, Ok(Query::MatchMultiTerm {
            field: foo_field,
            term_selector: TermSelector::Prefix("bar".to_string()),
            scorer: TermScorer::default_with_boost(2.0f64),
        }));
    }

    #[test]
    fn test_gives_error_for_incorrect_type() {
        // Array
        let query = parse(&Json::from_str("
        [
            \"foo\"
        ]
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Integer
        let query = parse(&Json::from_str("
        123
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));

        // Float
        let query = parse(&Json::from_str("
        123.1234
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedObject));
    }

    #[test]
    fn test_gives_error_for_incorrect_boost_type() {
        // String
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": \"2\"
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));

        // Array
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": [2]
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));

        // Object
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"boost\": {
                    \"value\": 2
                }
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedFloat));
    }

    #[test]
    fn test_gives_error_for_missing_value() {
        let query = parse(&Json::from_str("
        {
            \"foo\": {
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedKey("value")));
    }

    #[test]
    fn test_gives_error_for_extra_key() {
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\"
            },
            \"hello\": \"world\"
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::ExpectedSingleKey));
    }

    #[test]
    fn test_gives_error_for_extra_inner_key() {
        let query = parse(&Json::from_str("
        {
            \"foo\": {
                \"query\": \"bar\",
                \"hello\": \"world\"
            }
        }
        ").unwrap());

        assert_eq!(query.err(), Some(QueryParseError::UnrecognisedKey("hello".to_string())));
    }
}
