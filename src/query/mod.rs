pub mod parse;

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
    InvalidQueryOperator,
    InvalidQueryBoost,
}


#[derive(Debug, PartialEq)]
pub enum QueryOperator {
    Or,
    And,
}

impl Default for QueryOperator {
    fn default() -> QueryOperator {
        QueryOperator::Or
    }
}


#[derive(Debug, PartialEq)]
pub enum Query {
    MatchAll{boost: f64},
    Match{field: String, query: String, operator: QueryOperator, boost: f64},
    MultiMatch{fields: Vec<String>, query: String, operator: QueryOperator, boost: f64},
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


fn json_to_string(json: &Json) -> Option<String> {
    // Temporary hack to handle strings packed in arrays
    match *json {
        Json::String(ref s) => Some(s.clone()),
        Json::Array(ref a) => {
            let mut s = String::new();
            let mut have_value = false;

            for i in a {
                if let Some(is) = json_to_string(i) {
                    s.push_str(&is);
                    have_value = true;
                }
            }

            if have_value {
                Some(s)
            } else {
                None
            }
         }
        _ => None
    }
}


impl Query {
    pub fn rank(&self, doc: &Document) -> Option<f64> {
        match *self {
            Query::MatchAll{boost} => Some(boost),
            Query::Match{ref field, ref query, ref operator, boost} => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    let mut field_value = json_to_string(field_value).unwrap().to_lowercase();
                    let mut query = query.to_lowercase();

                    if field_value.contains(&query) {
                        return Some(boost);
                    }
                }

                None
            }
            Query::MultiMatch{ref fields, ref query, ref operator, boost} => {
                let obj = doc.data.as_object().unwrap();

                for field in fields.iter() {
                    if let Some(field_value) = obj.get(field) {
                        let mut field_value = json_to_string(field_value).unwrap().to_lowercase();
                        let mut query = query.to_lowercase();

                        if field_value.contains(&query) {
                            return Some(boost);
                        }
                    }
                }

                None
            }
            Query::Filtered{ref query, ref filter} => {
                if filter.matches(doc) {
                    return query.rank(doc)
                } else {
                    None
                }
            }
        }
    }

    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Query::MatchAll{ref boost} => true,
            Query::Match{ref field, ref query, ref operator, ref boost} => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    let mut field_value = field_value.as_string().unwrap().to_lowercase();
                    let mut query = query.to_lowercase();

                    return field_value.contains(&query);
                }

                false
            }
            Query::MultiMatch{ref fields, ref query, ref operator, ref boost} => {
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
