use rustc_serialize::json::Json;

use super::Document;


#[derive(Debug)]
pub enum Filter {
    Term(String, Json),
    Prefix(String, String),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}


impl Filter {
    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Filter::Term(ref field, ref value) => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    return field_value == value;
                }

                false
            }
            Filter::Prefix(ref field, ref value) => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    if let Json::String(ref field_value) = *field_value {
                        return field_value.starts_with(value);
                    }
                }

                false
            }
            Filter::And(ref filters) => {
                for filter in filters.iter() {
                    if !filter.matches(doc) {
                        return false;
                    }
                }

                true
            }
            Filter::Or(ref filters) => {
                for filter in filters.iter() {
                    if filter.matches(doc) {
                        return true;
                    }
                }

                false
            }
            Filter::Not(ref filter) => !filter.matches(doc),
        }
    }
}

pub fn parse_filter(json: &Json) -> Filter {
    let filter_json = json.as_object().unwrap();
    let first_key = filter_json.keys().nth(0).unwrap();

    if first_key == "term" {
        let filter_json = filter_json.get("term").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();

        Filter::Term(first_key.clone(), filter_json.get(first_key).unwrap().clone())
    } else if first_key == "prefix" {
        let filter_json = filter_json.get("prefix").unwrap().as_object().unwrap();
        let first_key = filter_json.keys().nth(0).unwrap();
        let value = filter_json.get(first_key).unwrap().as_string().unwrap();

        Filter::Prefix(first_key.clone(), value.to_owned())
    } else if first_key == "and" {
        Filter::And(filter_json.get("and").unwrap()
                               .as_array().unwrap()
                               .iter().map(|f| parse_filter(f))
                               .collect::<Vec<_>>(),)
    } else if first_key == "or" {
        Filter::Or(filter_json.get("or").unwrap()
                               .as_array().unwrap()
                               .iter().map(|f| parse_filter(f))
                               .collect::<Vec<_>>(),)
    } else if first_key == "not" {
        Filter::Not(Box::new(parse_filter(filter_json.get("not").unwrap())))
    } else {
        Filter::Term("not".to_owned(), Json::String("implemented".to_owned()))
    }
}

#[derive(Debug)]
pub enum Query {
    Match{field: String, query: String},
    MultiMatch{fields: Vec<String>, query: String},
    Filtered{query: Box<Query>, filter: Box<Filter>},
}

pub fn parse_match_query(json: &Json) -> Query {
    let query_json = json.as_object().unwrap();

    Query::Match {
        field: query_json.get("field").unwrap().as_string().unwrap().to_owned(),
        query: query_json.get("query").unwrap().as_string().unwrap().to_owned(),
    }
}

pub fn parse_multi_match_query(json: &Json) -> Query {
    let query_json = json.as_object().unwrap();

    Query::MultiMatch {
        // Convert "fields" into a Vec<String>
        fields: query_json.get("fields").unwrap()
                          .as_array().unwrap()
                          .iter().map(|s| s.as_string().unwrap().to_owned())
                          .collect::<Vec<_>>(),
        query: query_json.get("query").unwrap().as_string().unwrap().to_owned(),
    }
}

pub fn parse_filtered_query(json: &Json) -> Query {
    let query_json = json.as_object().unwrap();

    Query::Filtered {
        filter: Box::new(parse_filter(query_json.get("filter").unwrap())),
        query: Box::new(parse_query(query_json.get("query").unwrap())),
    }
}

pub fn parse_query(json: &Json) -> Query {
    let query_json = json.as_object().unwrap();
    let first_key = query_json.keys().nth(0).unwrap();

    if first_key == "match" {
        let inner_query = query_json.get("match").unwrap();
        parse_match_query(inner_query)
    } else if first_key == "multi_match" {
        let inner_query = query_json.get("multi_match").unwrap();
        parse_multi_match_query(inner_query)
    } else if first_key == "filtered" {
        let inner_query = query_json.get("filtered").unwrap();
        parse_filtered_query(inner_query)
    } else {
        // TODO
        Query::Match {
            field: "not".to_owned(),
            query: "implemented".to_owned(),
        }
    }
}
