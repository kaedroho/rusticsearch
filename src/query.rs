use rustc_serialize::json::Json;

use super::Document;


#[derive(Debug, PartialEq)]
pub enum FilterParseError {
    ExpectedObject,
    ExpectedString,
    ExpectedArray,
    UnknownFilterType(String),
    NoFilter,
}


#[derive(Debug, PartialEq)]
pub enum Filter {
    Term{field: String, value: Json},
    Terms{field: String, values: Vec<Json>},
    Prefix{field: String, value: String},
    Missing{field: String},
    And{children: Vec<Filter>},
    Or{children: Vec<Filter>},
    Not{child: Box<Filter>},
}


#[derive(Debug, PartialEq)]
pub enum QueryParseError {
    ExpectedObject,
    ExpectedString,
    ExpectedArray,
    UnknownQueryType(String),
    NoQuery,
    FilteredNoFilter,
    FilteredNoQuery,
    FilterParseError(FilterParseError),
    MissingQueryString,
    MultiMatchMissingFields,
}


#[derive(Debug, PartialEq)]
pub enum Query {
    MatchAll,
    Match{field: String, query: String},
    MultiMatch{fields: Vec<String>, query: String},
    Filtered{query: Box<Query>, filter: Box<Filter>},
}


impl Filter {
    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Filter::Term{ref field, ref value} => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    return field_value == value;
                }

                false
            }
            Filter::Terms{ref field, ref values} => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    for value in values.iter() {
                        if field_value == value {
                            return true;
                        }
                    }
                }

                false
            }
            Filter::Prefix{ref field, ref value} => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    if let Json::String(ref field_value) = *field_value {
                        return field_value.starts_with(value);
                    }
                }

                false
            }
            Filter::Missing{ref field} => {
                let obj = doc.data.as_object().unwrap();

                match obj.get(field) {
                    Some(&Json::Null) => true,
                    None => true,
                    _ => false,
                }
            }
            Filter::And{ref children} => {
                for child in children.iter() {
                    if !child.matches(doc) {
                        return false;
                    }
                }

                true
            }
            Filter::Or{ref children} => {
                for child in children.iter() {
                    if child.matches(doc) {
                        return true;
                    }
                }

                false
            }
            Filter::Not{ref child} => !child.matches(doc),
        }
    }
}

pub fn parse_filter(json: &Json) -> Result<Filter, FilterParseError> {
    let filter_json = try!(json.as_object().ok_or(FilterParseError::ExpectedObject));
    let first_key = try!(filter_json.keys().nth(0).ok_or(FilterParseError::NoFilter));

    if first_key == "term" {
        let filter_json = filter_json.get("term").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();

        Ok(Filter::Term{
            field: first_key.clone(),
            value: filter_json.get(first_key).unwrap().clone()
        })
    } else if first_key == "terms" {
        let filter_json = filter_json.get("terms").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();

        Ok(Filter::Terms{
            field: first_key.clone(),
            values: filter_json.get(first_key).unwrap().as_array().unwrap().clone()
        })
    } else if first_key == "prefix" {
        let filter_json = filter_json.get("prefix").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();
        let value = filter_json.get(first_key).unwrap().as_string().unwrap();

        Ok(Filter::Prefix{
            field: first_key.clone(),
            value: value.to_owned()
        })
    } else if first_key == "missing" {
        let filter_json = filter_json.get("missing").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();

        Ok(Filter::Missing{
            field: first_key.clone()
        })
    } else if first_key == "and" {
        Ok(Filter::And{
            children: filter_json.get("and").unwrap()
                       .as_array().unwrap()
                       .iter().map(|f| parse_filter(f).unwrap())
                       .collect::<Vec<_>>()
        })
    } else if first_key == "or" {
        Ok(Filter::Or{
            children: filter_json.get("or").unwrap()
                       .as_array().unwrap()
                       .iter().map(|f| parse_filter(f).unwrap())
                       .collect::<Vec<_>>()
        })
    } else if first_key == "not" {
        Ok(Filter::Not{
            child: Box::new(parse_filter(filter_json.get("not").unwrap()).unwrap())
        })
    } else {
        Err(FilterParseError::UnknownFilterType(first_key.clone()))
    }
}

impl Query {
    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Query::MatchAll => true,
            Query::Match{ref field, ref query} => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    let mut field_value = field_value.as_string().unwrap().to_lowercase();
                    let mut query = query.to_lowercase();

                    return field_value.contains(&query);
                }

                false
            }
            Query::MultiMatch{ref fields, ref query} => {
                let obj = doc.data.as_object().unwrap();

                for field in fields.iter() {
                    if let Some(field_value) = obj.get(field) {
                        let mut field_value = field_value.as_string().unwrap().to_lowercase();
                        let mut query = query.to_lowercase();

                        if field_value.contains(&query) {
                            return true;
                        }
                    }
                }

                false
            }
            Query::Filtered{ref query, ref filter} => {
                if filter.matches(doc) {
                    query.matches(doc)
                } else {
                    false
                }
            }
        }
    }
}

pub fn parse_match_query(json: &Json) -> Result<Query, QueryParseError> {
    let json_object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));
    let first_key = try!(json_object.keys().nth(0).ok_or(QueryParseError::NoQuery));

    Ok(Query::Match {
        field: first_key.clone(),
        query: json_object.get(first_key).unwrap().as_string().unwrap().to_owned(),
    })
}

pub fn parse_multi_match_query(json: &Json) -> Result<Query, QueryParseError> {
    let json_object = try!(json.as_object().ok_or(QueryParseError::ExpectedObject));

    // Convert "fields" into a Vec<String>
    let fields_json = try!(json_object.get("fields").ok_or(QueryParseError::MultiMatchMissingFields));
    let fields = try!(fields_json.as_array().ok_or(QueryParseError::ExpectedArray))
                      .iter().map(|s| s.as_string().unwrap().to_owned())
                      .collect::<Vec<_>>();

    let query_json = try!(json_object.get("query").ok_or(QueryParseError::MissingQueryString));
    let query = try!(query_json.as_string().ok_or(QueryParseError::ExpectedString)).to_owned();

    Ok(Query::MultiMatch {
        fields: fields,
        query: query,
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
        Ok(Query::MatchAll)
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
    use super::{Query, Filter, QueryParseError, parse_query};

    #[test]
    fn test_match_all_query() {
        let query = parse_query(&Json::from_str("
            {
                \"match_all\": {}
            }
        ").unwrap());

        assert_eq!(query, Ok(Query::MatchAll))
    }

    #[test]
    fn test_match_query() {
        let query = parse_query(&Json::from_str("
            {
                \"match\": {
                    \"title\": \"Hello world!\"
                }
            }
        ").unwrap());

        assert_eq!(query, Ok(Query::Match{
            field: "title".to_owned(),
            query: "Hello world!".to_owned(),
        }))
    }

    #[test]
    fn test_match_query_without_field() {
        let query = parse_query(&Json::from_str("
            {
                \"match\": {}
            }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::NoQuery))
    }

    #[test]
    fn test_multi_match_query() {
        let query = parse_query(&Json::from_str("
            {
                \"multi_match\": {
                    \"fields\": [\"title\", \"body\"],
                    \"query\": \"Hello world!\"
                }
            }
        ").unwrap());

        assert_eq!(query, Ok(Query::MultiMatch{
            fields: vec!["title".to_owned(), "body".to_owned()],
            query: "Hello world!".to_owned(),
        }))
    }

    #[test]
    fn test_multi_match_query_without_fields() {
        let query = parse_query(&Json::from_str("
            {
                \"multi_match\": {
                    \"query\": \"Hello world!\"
                }
            }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::MultiMatchMissingFields))
    }

    #[test]
    fn test_multi_match_query_without_query() {
        let query = parse_query(&Json::from_str("
            {
                \"multi_match\": {
                    \"fields\": [\"title\", \"body\"]
                }
            }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::MissingQueryString))
    }

    #[test]
    fn test_filtered_query() {
        let query = parse_query(&Json::from_str("
            {
                \"filtered\": {
                    \"query\": {
                        \"match\": {
                            \"title\": \"Hello world!\"
                        }
                    },
                    \"filter\": {
                        \"term\": {
                            \"date\": \"2016-01-25\"
                        }
                    }
                }
            }
        ").unwrap());

        assert_eq!(query, Ok(Query::Filtered{
            query: Box::new(Query::Match{
                field: "title".to_owned(),
                query: "Hello world!".to_owned(),
            }),
            filter: Box::new(Filter::Term{
                field: "date".to_owned(),
                value: Json::from_str("\"2016-01-25\"").unwrap(),
            })
        }))
    }

    #[test]
    fn test_filtered_query_without_query() {
        let query = parse_query(&Json::from_str("
            {
                \"filtered\": {
                    \"filter\": {
                        \"term\": {
                            \"date\": \"2016-01-25\"
                        }
                    }
                }
            }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::FilteredNoQuery))
    }

    #[test]
    fn test_filtered_query_without_filter() {
        let query = parse_query(&Json::from_str("
            {
                \"filtered\": {
                    \"query\": {
                        \"match\": {
                            \"title\": \"Hello world!\"
                        }
                    }
                }
            }
        ").unwrap());

        assert_eq!(query, Err(QueryParseError::FilteredNoFilter))
    }
}
