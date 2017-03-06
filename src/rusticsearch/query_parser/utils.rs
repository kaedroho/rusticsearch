use serde_json::Value as Json;
use kite::term::Term;

use query_parser::QueryParseError;


pub fn parse_string(json: &Json) -> Result<String, QueryParseError> {
    match *json {
        Json::String(ref string) => Ok(string.clone()),
        _ => Err(QueryParseError::ExpectedString),
    }
}


pub fn parse_float(json: &Json) -> Result<f64, QueryParseError> {
    match json {
        &Json::Number(ref number) => {
            match number.as_f64() {
                Some(val) => Ok(val),
                None => Err(QueryParseError::ExpectedFloat),
            }
        }
        _ => Err(QueryParseError::ExpectedFloat),
    }
}


#[derive(Debug)]
pub enum Operator {
    Or,
    And,
}


pub fn parse_operator(json: &Json) -> Result<Operator, QueryParseError> {
    match *json {
        Json::String(ref value) => {
            match value.as_ref() {
                "or" => Ok(Operator::Or),
                "and" => Ok(Operator::And),
                _ => return Err(QueryParseError::InvalidOperator),
            }
        }
        _ => return Err(QueryParseError::InvalidOperator),
    }
}


pub fn parse_field_and_boost(json: &Json) -> Result<(String, f64), QueryParseError> {
    let string = try!(parse_string(json));

    let split = string.split('^').collect::<Vec<_>>();
    if split.len() == 1 {
        return Ok((string.clone(), 1.0f64));
    } else {
        let field_name = split[0].to_owned();
        let boost: f64 = split[1].parse().unwrap_or(1.0f64);
        return Ok((field_name, boost));
    }
}


pub fn json_value_to_term(json: &Json) -> Option<Term> {
    match json {
        &Json::String(ref string) => Some(Term::from_string(string)),
        &Json::Bool(value) => Some(Term::from_boolean(value)),
        &Json::Number(ref value) => {
            match value.as_i64() {
                Some(value) => Some(Term::from_integer(value)),
                None => None,
            }
        }
        &Json::Null => None,
        &Json::Array(_) => None,
        &Json::Object(_) => None,
    }
}
