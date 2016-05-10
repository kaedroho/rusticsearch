pub mod parse;

use rustc_serialize::json::Json;

use super::analysis::Analyzer;
use super::{Document, Value};


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
    Term {
        field: String,
        value: Value,
    },
    Terms {
        field: String,
        values: Vec<Value>,
    },
    Prefix {
        field: String,
        value: String,
    },
    Missing {
        field: String,
    },
    And {
        children: Vec<Filter>,
    },
    Or {
        children: Vec<Filter>,
    },
    Not {
        child: Box<Filter>,
    },
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
    InvalidInteger,
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
    MatchAll {
        boost: f64,
    },
    Match {
        fields: Vec<String>,
        query: String,
        operator: QueryOperator,
        boost: f64,
    },
    Bool {
        must: Vec<Query>,
        must_not: Vec<Query>,
        should: Vec<Query>,
        filter: Vec<Filter>,
        minimum_should_match: i32,
        boost: f64,
    },
}


impl Filter {
    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Filter::Term{ref field, ref value} => {
                if let Some(field_value) = doc.fields.get(field) {
                    return field_value == value;
                }

                false
            }
            Filter::Terms{ref field, ref values} => {
                if let Some(field_value) = doc.fields.get(field) {
                    for value in values.iter() {
                        if field_value == value {
                            return true;
                        }
                    }
                }

                false
            }
            Filter::Prefix{ref field, ref value} => {
                if let Some(field_value) = doc.fields.get(field) {
                    if let Value::String(ref field_value) = *field_value {
                        return field_value.starts_with(value);
                    }
                }

                false
            }
            Filter::Missing{ref field} => {
                match doc.fields.get(field) {
                    Some(&Value::Null) => true,
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


impl Query {
    pub fn rank(&self, doc: &Document) -> Option<f64> {
        match *self {
            Query::MatchAll{boost} => Some(boost),
            Query::Match{ref fields, ref query, ref operator, boost} => {
                let query = Analyzer::Standard.run(query.clone());
                let mut matches = 0;

                for token in query.iter() {
                    let mut token_matched = false;

                    for field in fields.iter() {
                        if let Some(&Value::TSVector(ref field_value)) = doc.fields.get(field) {
                            for field_token in field_value.iter() {
                                if token == field_token {
                                    matches += 1;
                                    token_matched = true;
                                }
                            }
                        }
                    }

                    // All tokens must match for queries with and operator
                    if operator == &QueryOperator::And && !token_matched {
                        return None;
                    }
                }

                if matches > 0 {
                    Some(matches as f64)
                } else {
                    None
                }
            }
            Query::Bool{ref must, ref must_not, ref should, ref filter, minimum_should_match, boost} => {
                let mut total_score: f64 = 0.0;

                // Must not
                for query in must_not {
                    if query.matches(doc) {
                        return None;
                    }
                }

                // Filter
                for filter in filter {
                    if !filter.matches(doc) {
                        return None;
                    }
                }

                // Must
                for query in must {
                    match query.rank(doc) {
                        Some(score) => {
                            total_score += score;
                        }
                        None => return None,
                    }
                }

                // Should
                let mut should_matched: i32 = 0;
                for query in should {
                    if let Some(score) = query.rank(doc) {
                        should_matched += 1;
                        total_score += score;
                    }
                }

                if should_matched < minimum_should_match {
                    return None;
                }

                // Return average score of matched queries
                Some((total_score * boost) / (must.len() + should.len()) as f64)
            }
        }
    }

    pub fn matches(&self, doc: &Document) -> bool {
        match *self {
            Query::MatchAll{ref boost} => true,
            Query::Match{ref fields, ref query, ref operator, ref boost} => {
                for field in fields.iter() {
                    if let Some(&Value::String(ref field_value)) = doc.fields.get(field) {
                        let mut field_value = field_value.to_lowercase();
                        let mut query = query.to_lowercase();

                        if field_value.contains(&query) {
                            return true;
                        }
                    }
                }

                false
            }
            Query::Bool{ref must, ref must_not, ref should, ref filter, minimum_should_match, boost} => {
                // Must not
                for query in must_not {
                    if query.matches(doc) {
                        return false;
                    }
                }

                // Filter
                for filter in filter {
                    if !filter.matches(doc) {
                        return false;
                    }
                }

                // Must
                for query in must {
                    if !query.matches(doc) {
                        return false;
                    }
                }

                // Should
                if minimum_should_match > 0 {
                    let mut should_matched: i32 = 0;
                    for query in should {
                        if query.matches(doc) {
                            should_matched += 1;

                            if should_matched >= minimum_should_match {
                                return true;
                            }
                        }
                    }

                    return false;
                }

                return true;
            }
        }
    }
}
