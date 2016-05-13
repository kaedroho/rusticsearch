use rustc_serialize::json::Json;

use query::parser::QueryParseError;


pub fn parse_string(json: &Json) -> Result<String, QueryParseError> {
    match *json {
        Json::String(ref string) => Ok(string.clone()),
        _ => Err(QueryParseError::ExpectedString),
    }
}


pub fn parse_float(json: &Json) -> Result<f64, QueryParseError> {
    match *json {
        Json::F64(val) => Ok(val),
        Json::I64(val) => Ok(val as f64),
        Json::U64(val) => Ok(val as f64),
        _ => Err(QueryParseError::ExpectedFloat),
    }
}


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
