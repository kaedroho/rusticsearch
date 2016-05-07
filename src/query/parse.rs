use rustc_serialize::json::Json;

use Value;
use super::{Query, Filter, QueryParseError, FilterParseError, QueryOperator};


pub fn parse_filter(json: &Json) -> Result<Filter, FilterParseError> {
    let filter_json = try!(json.as_object().ok_or(FilterParseError::ExpectedObject));
    let first_key = try!(filter_json.keys().nth(0).ok_or(FilterParseError::NoFilter));

    if first_key == "term" {
        let filter_json = filter_json.get("term").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();

        Ok(Filter::Term {
            field: first_key.clone(),
            value: Value::from_json(filter_json.get(first_key).unwrap()),
        })
    } else if first_key == "terms" {
        let filter_json = filter_json.get("terms").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();

        Ok(Filter::Terms {
            field: first_key.clone(),
            values: filter_json.get(first_key)
                               .unwrap()
                               .as_array()
                               .unwrap()
                               .iter()
                               .map(|v| Value::from_json(v))
                               .collect::<Vec<_>>(),
        })
    } else if first_key == "prefix" {
        let filter_json = filter_json.get("prefix").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();
        let value = filter_json.get(first_key).unwrap().as_string().unwrap();

        Ok(Filter::Prefix {
            field: first_key.clone(),
            value: value.to_owned(),
        })
    } else if first_key == "missing" {
        let filter_json = filter_json.get("missing").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();

        Ok(Filter::Missing { field: first_key.clone() })
    } else if first_key == "and" {
        Ok(Filter::And {
            children: filter_json.get("and")
                                 .unwrap()
                                 .as_array()
                                 .unwrap()
                                 .iter()
                                 .map(|f| parse_filter(f).unwrap())
                                 .collect::<Vec<_>>(),
        })
    } else if first_key == "or" {
        Ok(Filter::Or {
            children: filter_json.get("or")
                                 .unwrap()
                                 .as_array()
                                 .unwrap()
                                 .iter()
                                 .map(|f| parse_filter(f).unwrap())
                                 .collect::<Vec<_>>(),
        })
    } else if first_key == "not" {
        Ok(Filter::Not { child: Box::new(parse_filter(filter_json.get("not").unwrap()).unwrap()) })
    } else {
        Err(FilterParseError::UnknownFilterType(first_key.clone()))
    }
}

pub fn parse_query_operator(json: Option<&Json>) -> Result<QueryOperator, QueryParseError> {
    match json {
        Some(json) => {
            match *json {
                Json::String(ref value) => {
                    match value.as_ref() {
                        "or" => Ok(QueryOperator::Or),
                        "and" => Ok(QueryOperator::And),
                        _ => return Err(QueryParseError::InvalidQueryOperator),
                    }
                }
                _ => return Err(QueryParseError::InvalidQueryOperator),
            }
        }
        None => Ok(QueryOperator::default()),
    }
}

pub fn parse_query_boost(json: Option<&Json>) -> Result<f64, QueryParseError> {
    match json {
        Some(json) => {
            match *json {
                Json::F64(value) => return Ok(value),
                Json::I64(value) => return Ok(value as f64),
                Json::U64(value) => return Ok(value as f64),
                _ => return Err(QueryParseError::InvalidQueryBoost),
            }
        }
        None => Ok(1.0f64),
    }
}

pub fn parse_match_all_query(json: &Json) -> Result<Query, QueryParseError> {
    let json_object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    Ok(Query::MatchAll { boost: try!(parse_query_boost(json_object.get("boost"))) })
}

pub fn parse_match_query(json: &Json) -> Result<Query, QueryParseError> {
    let json_object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));
    let first_key = try!(json_object.keys().nth(0).ok_or(QueryParseError::NoQuery));

    match json_object.get(first_key).unwrap() {
        &Json::String(ref query) => {
            Ok(Query::Match {
                field: first_key.clone(),
                query: query.to_owned(),
                operator: QueryOperator::default(),
                boost: 1.0f64,
            })
        }
        &Json::Object(ref object) => {
            Ok(Query::Match {
                field: first_key.clone(),
                query: object.get("query").unwrap().as_string().unwrap().to_owned(),
                operator: try!(parse_query_operator(object.get("operator"))),
                boost: try!(parse_query_boost(object.get("boost"))),
            })
        }
        // TODO: We actually expect string or object
        _ => Err(QueryParseError::ExpectedString),
    }
}

pub fn parse_multi_match_query(json: &Json) -> Result<Query, QueryParseError> {
    let json_object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Convert "fields" into a Vec<String>
    let fields_json = try!(json_object.get("fields")
                                      .ok_or(QueryParseError::MultiMatchMissingFields));
    let fields = try!(fields_json.as_array().ok_or(QueryParseError::ExpectedArray))
                     .iter()
                     .map(|s| s.as_string().unwrap().to_owned())
                     .collect::<Vec<_>>();

    let query_json = try!(json_object.get("query").ok_or(QueryParseError::MissingQueryString));
    let query = try!(query_json.as_string().ok_or(QueryParseError::ExpectedString)).to_owned();

    Ok(Query::MultiMatch {
        fields: fields,
        query: query,
        operator: try!(parse_query_operator(json_object.get("operator"))),
        boost: try!(parse_query_boost(json_object.get("boost"))),
    })
}

pub fn parse_filtered_query(json: &Json) -> Result<Query, QueryParseError> {
    let json_object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    let filter_json = try!(json_object.get("filter").ok_or(QueryParseError::FilteredNoFilter));
    let filter = match parse_filter(filter_json) {
        Ok(filter) => filter,
        Err(err) => return Err(QueryParseError::FilterParseError(err)),
    };

    let query_json = try!(json_object.get("query").ok_or(QueryParseError::FilteredNoQuery));
    let query = try!(parse_query(query_json));

    Ok(Query::Filtered {
        filter: Box::new(filter),
        query: Box::new(query),
    })
}

pub fn parse_query(json: &Json) -> Result<Query, QueryParseError> {
    let json_object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));
    let first_key = try!(json_object.keys().nth(0).ok_or(QueryParseError::NoQuery));

    if first_key == "match_all" {
        let inner_query = json_object.get("match_all").unwrap();
        Ok(try!(parse_match_all_query(inner_query)))
    } else if first_key == "match" {
        let inner_query = json_object.get("match").unwrap();
        Ok(try!(parse_match_query(inner_query)))
    } else if first_key == "multi_match" {
        let inner_query = json_object.get("multi_match").unwrap();
        Ok(try!(parse_multi_match_query(inner_query)))
    } else if first_key == "filtered" {
        let inner_query = json_object.get("filtered").unwrap();
        Ok(try!(parse_filtered_query(inner_query)))
    } else {
        Err(QueryParseError::UnknownQueryType(first_key.clone()))
    }
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;
    use query::{Query, Filter, QueryParseError, QueryOperator};
    use super::parse_query;

    #[test]
    fn test_match_all_query() {
        let query = parse_query(&Json::from_str("
            {
                \"match_all\": \
                                                 {}
            }
        ")
                                     .unwrap());

        assert_eq!(query, Ok(Query::MatchAll { boost: 1.0f64 }))
    }

    #[test]
    fn test_match_all_query_boost() {
        let query = parse_query(&Json::from_str("
            {
                \"match_all\": \
                                                 {
                    \"boost\": 1.234
                \
                                                 }
            }
        ")
                                     .unwrap());

        assert_eq!(query, Ok(Query::MatchAll { boost: 1.234f64 }))
    }

    #[test]
    fn test_match_all_query_invalid_boost() {
        let query = parse_query(&Json::from_str("
            {
                \"match_all\": \
                                                 {
                    \"boost\": \"foo\"
                \
                                                 }
            }
        ")
                                     .unwrap());

        assert_eq!(query, Err(QueryParseError::InvalidQueryBoost))
    }

    #[test]
    fn test_match_query() {
        let query = parse_query(&Json::from_str("
            {
                \"match\": {
                    \
                                                 \"title\": \"Hello world!\"
                }
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query,
                   Ok(Query::Match {
                       field: "title".to_owned(),
                       query: "Hello world!".to_owned(),
                       operator: QueryOperator::Or,
                       boost: 1.0f64,
                   }))
    }

    #[test]
    fn test_match_dict_config() {
        let query = parse_query(&Json::from_str("
            {
                \"match\": {
                    \
                                                 \"title\": {
                        \
                                                 \"query\": \"Hello world!\"
                    \
                                                 }
                }
            }
        ")
                                     .unwrap());

        assert_eq!(query,
                   Ok(Query::Match {
                       field: "title".to_owned(),
                       query: "Hello world!".to_owned(),
                       operator: QueryOperator::Or,
                       boost: 1.0f64,
                   }))
    }

    #[test]
    fn test_match_and_operator() {
        let query = parse_query(&Json::from_str("
            {
                \"match\": {
                    \
                                                 \"title\": {
                        \
                                                 \"query\": \"Hello world!\",
                        \
                                                 \"operator\": \"and\"
                    }
                \
                                                 }
            }
        ")
                                     .unwrap());

        assert_eq!(query,
                   Ok(Query::Match {
                       field: "title".to_owned(),
                       query: "Hello world!".to_owned(),
                       operator: QueryOperator::And,
                       boost: 1.0f64,
                   }))
    }

    #[test]
    fn test_match_invalid_operator() {
        let query = parse_query(&Json::from_str("
            {
                \"match\": {
                    \
                                                 \"title\": {
                        \
                                                 \"query\": \"Hello world!\",
                        \
                                                 \"operator\": \"invalid\"
                    \
                                                 }
                }
            }
        ")
                                     .unwrap());

        assert_eq!(query, Err(QueryParseError::InvalidQueryOperator))
    }

    #[test]
    fn test_match_boost() {
        let query = parse_query(&Json::from_str("
            {
                \"match\": {
                    \
                                                 \"title\": {
                        \
                                                 \"query\": \"Hello world!\",
                        \
                                                 \"boost\": 1.234
                    }
                \
                                                 }
            }
        ")
                                     .unwrap());

        assert_eq!(query,
                   Ok(Query::Match {
                       field: "title".to_owned(),
                       query: "Hello world!".to_owned(),
                       operator: QueryOperator::Or,
                       boost: 1.234f64,
                   }))
    }

    #[test]
    fn test_match_query_without_field() {
        let query = parse_query(&Json::from_str("
            {
                \"match\": {}
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query, Err(QueryParseError::NoQuery))
    }

    #[test]
    fn test_multi_match_query() {
        let query = parse_query(&Json::from_str("
            {
                \
                                                 \"multi_match\": {
                    \
                                                 \"fields\": [\"title\", \"body\"],
                    \
                                                 \"query\": \"Hello world!\"
                }
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query,
                   Ok(Query::MultiMatch {
                       fields: vec!["title".to_owned(), "body".to_owned()],
                       query: "Hello world!".to_owned(),
                       operator: QueryOperator::Or,
                       boost: 1.0f64,
                   }))
    }

    #[test]
    fn test_multi_match_and_operator() {
        let query = parse_query(&Json::from_str("
            {
                \
                                                 \"multi_match\": {
                    \
                                                 \"fields\": [\"title\", \"body\"],
                    \
                                                 \"query\": \"Hello world!\",
                    \
                                                 \"operator\": \"and\"
                }
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query,
                   Ok(Query::MultiMatch {
                       fields: vec!["title".to_owned(), "body".to_owned()],
                       query: "Hello world!".to_owned(),
                       operator: QueryOperator::And,
                       boost: 1.0f64,
                   }))
    }

    #[test]
    fn test_multi_match_invalid_operator() {
        let query = parse_query(&Json::from_str("
            {
                \
                                                 \"multi_match\": {
                    \
                                                 \"fields\": [\"title\", \"body\"],
                    \
                                                 \"query\": \"Hello world!\",
                    \
                                                 \"operator\": \"invalid\"
                }
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query, Err(QueryParseError::InvalidQueryOperator))
    }

    #[test]
    fn test_multi_match_boost() {
        let query = parse_query(&Json::from_str("
            {
                \
                                                 \"multi_match\": {
                    \
                                                 \"fields\": [\"title\", \"body\"],
                    \
                                                 \"query\": \"Hello world!\",
                    \
                                                 \"boost\": 1.234
                }
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query,
                   Ok(Query::MultiMatch {
                       fields: vec!["title".to_owned(), "body".to_owned()],
                       query: "Hello world!".to_owned(),
                       operator: QueryOperator::Or,
                       boost: 1.234f64,
                   }))
    }

    #[test]
    fn test_multi_match_query_without_fields() {
        let query = parse_query(&Json::from_str("
            {
                \
                                                 \"multi_match\": {
                    \
                                                 \"query\": \"Hello world!\"
                }
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query, Err(QueryParseError::MultiMatchMissingFields))
    }

    #[test]
    fn test_multi_match_query_without_query() {
        let query = parse_query(&Json::from_str("
            {
                \
                                                 \"multi_match\": {
                    \
                                                 \"fields\": [\"title\", \"body\"]
                \
                                                 }
            }
        ")
                                     .unwrap());

        assert_eq!(query, Err(QueryParseError::MissingQueryString))
    }

    #[test]
    fn test_filtered_query() {
        let query = parse_query(&Json::from_str("
            {
                \"filtered\": {
                    \
                                                 \"query\": {
                        \
                                                 \"match\": {
                            \
                                                 \"title\": \"Hello world!\"
                        \
                                                 }
                    },
                    \
                                                 \"filter\": {
                        \
                                                 \"term\": {
                            \
                                                 \"date\": \"2016-01-25\"
                        \
                                                 }
                    }
                }
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query,
                   Ok(Query::Filtered {
                       query: Box::new(Query::Match {
                           field: "title".to_owned(),
                           query: "Hello world!".to_owned(),
                           operator: QueryOperator::Or,
                           boost: 1.0f64,
                       }),
                       filter: Box::new(Filter::Term {
                           field: "date".to_owned(),
                           value: Json::from_str("\"2016-01-25\"").unwrap(),
                       }),
                   }))
    }

    #[test]
    fn test_filtered_query_without_query() {
        let query = parse_query(&Json::from_str("
            {
                \"filtered\": {
                    \
                                                 \"filter\": {
                        \
                                                 \"term\": {
                            \
                                                 \"date\": \"2016-01-25\"
                        \
                                                 }
                    }
                }
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query, Err(QueryParseError::FilteredNoQuery))
    }

    #[test]
    fn test_filtered_query_without_filter() {
        let query = parse_query(&Json::from_str("
            {
                \"filtered\": {
                    \
                                                 \"query\": {
                        \
                                                 \"match\": {
                            \
                                                 \"title\": \"Hello world!\"
                        \
                                                 }
                    }
                }
            \
                                                 }
        ")
                                     .unwrap());

        assert_eq!(query, Err(QueryParseError::FilteredNoFilter))
    }
}
