use super::Document;

use rustc_serialize::json::Json;


#[derive(Debug)]
enum Filter {
    Not(Box<Filter>),
    Or(Vec<Filter>),
    And(Vec<Filter>),
    Term(String, String),
}


impl Filter {
    fn matches(&self, doc: &Document) -> bool {
        match *self {
            Filter::Not(ref filter) => !filter.matches(doc),
            Filter::Or(ref filters) => {
                for filter in filters.iter() {
                    if filter.matches(doc) {
                        return true;
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
            Filter::Term(ref field, ref value) => {
                let obj = doc.data.as_object().unwrap();

                if let Some(field_value) = obj.get(field) {
                    if let Json::String(ref field_value) = *field_value {
                        return field_value == value;
                    }
                }

                false
            }
        }
    }
}
