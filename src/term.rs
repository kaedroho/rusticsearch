use rustc_serialize::json::Json;


#[derive(Debug, PartialEq)]
pub enum Term {
    String(String),
    TSVector(Vec<String>),
    Boolean(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Null,
}

impl Term {
    pub fn from_json(json: &Json) -> Term {
        // TODO: Should be aware of mappings
        match *json {
            Json::String(ref string) => Term::String(string.clone()),
            Json::Boolean(value) => Term::Boolean(value),
            Json::F64(value) => Term::F64(value),
            Json::I64(value) => Term::I64(value),
            Json::U64(value) => Term::U64(value),
            Json::Null => Term::Null,

            // These two are unsupported
            // TODO: Raise error
            Json::Array(_) => Term::Null,
            Json::Object(_) => Term::Null,
        }
    }

    pub fn as_json(&self) -> Json {
        match *self {
            Term::String(ref string) => Json::String(string.clone()),
            Term::TSVector(ref string) => Json::Null, // TODO
            Term::Boolean(value) => Json::Boolean(value),
            Term::F64(value) => Json::F64(value),
            Term::I64(value) => Json::I64(value),
            Term::U64(value) => Json::U64(value),
            Term::Null => Json::Null,
        }
    }
}
