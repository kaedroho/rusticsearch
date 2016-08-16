use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use rustc_serialize::json::Json;
use chrono::{DateTime, UTC};

use term::Term;
use token::Token;
use analysis::Analyzer;


#[derive(Debug, PartialEq)]
pub enum FieldType {
    String,
    Integer,
    Boolean,
    Date,
}


impl Default for FieldType {
    fn default() -> FieldType {
        FieldType::String
    }
}


#[derive(Debug)]
pub struct FieldMapping {
    data_type: FieldType,
    is_stored: bool,
    pub is_in_all: bool,
    boost: f64,
    analyzer: Analyzer,
}


impl Default for FieldMapping {
    fn default() -> FieldMapping {
        FieldMapping {
            data_type: FieldType::default(),
            is_stored: false,
            is_in_all: true,
            boost: 1.0f64,
            analyzer: Analyzer::Standard,
        }
    }
}


impl FieldMapping {
    pub fn process_value_for_index(&self, value: Json) -> Option<Vec<Token>> {
        if value == Json::Null {
            return None;
        }

        match self.data_type {
            FieldType::String => {
                match value {
                    Json::String(string) => {
                        // Analyzed strings become TSVectors. Unanalyzed strings become... strings
                        if self.analyzer == Analyzer::None {
                            Some(vec![Token{term: Term::String(string), position: 1}])
                        } else {
                            Some(self.analyzer.run(string))
                        }
                    }
                    Json::I64(num) => self.process_value_for_index(Json::String(num.to_string())),
                    Json::U64(num) => self.process_value_for_index(Json::String(num.to_string())),
                    Json::F64(num) => self.process_value_for_index(Json::String(num.to_string())),
                    Json::Array(array) => {
                        // Pack any strings into a vec, ignore nulls. Quit if we see anything else
                        let mut strings = Vec::new();

                        for item in array {
                            match item {
                                Json::String(string) => strings.push(string),
                                Json::Null => {}
                                _ => {
                                    return None;
                                }
                            }
                        }

                        self.process_value_for_index(Json::String(strings.join(" ")))
                    }
                    _ => None,
                }
            }
            FieldType::Integer => {
                match value {
                    Json::U64(num) => Some(vec![Token{term: Term::I64(num as i64), position: 1}]),
                    Json::I64(num) => Some(vec![Token{term: Term::I64(num), position: 1}]),
                    _ => None,
                }
            }
            FieldType::Boolean => Some(vec![Token{term: Term::Boolean(parse_boolean(&value)), position: 1}]),
            FieldType::Date => {
                match value {
                    Json::String(string) => {
                        let date_parsed = match string.parse::<DateTime<UTC>>() {
                            Ok(date_parsed) => date_parsed,
                            Err(error) => {
                                // TODO: Handle this properly
                                return None;
                            }
                        };

                        Some(vec![Token{term: Term::DateTime(date_parsed), position: 1}])
                    }
                    Json::U64(num) => {
                        // TODO needs to be interpreted as milliseconds since epoch
                        // This would really help: https://github.com/lifthrasiir/rust-chrono/issues/74
                        None
                    }
                    _ => None
                }
            }
        }
    }

    pub fn process_value_for_query(&self, value: Json) -> Option<Vec<Token>> {
        // Currently not different from process_value_for_index
        self.process_value_for_index(value)
    }
}


#[derive(Debug)]
pub struct Mapping {
    pub fields: HashMap<String, FieldMapping>,
}

impl Mapping {
    pub fn from_json(json: &Json) -> Mapping {
        let json = json.as_object().unwrap();
        let properties_json = json.get("properties").unwrap().as_object().unwrap();

        // Parse fields
        let mut fields = HashMap::new();
        for (field_name, field_mapping_json) in properties_json.iter() {
            fields.insert(field_name.clone(),
                          FieldMapping::from_json(field_mapping_json));
        }

        Mapping { fields: fields }
    }
}


fn parse_boolean(json: &Json) -> bool {
    match *json {
        Json::Boolean(val) => val,
        Json::String(ref s) => {
            match s.as_ref() {
                "yes" => true,
                "no" => false,
                _ => {
                    warn!("bad boolean value {:?}", s);
                    false
                }
            }
        }
        _ => {
            // TODO: Raise error
            warn!("bad boolean value {:?}", json);
            false
        }
    }
}


impl FieldMapping {
    pub fn from_json(json: &Json) -> FieldMapping {
        let json = json.as_object().unwrap();
        let mut field_mapping = FieldMapping::default();

        for (key, value) in json.iter() {
            match key.as_ref() {
                "type" => {
                    let type_name = value.as_string().unwrap();

                    field_mapping.data_type = match type_name.as_ref() {
                        "string" => FieldType::String,
                        "integer" => FieldType::Integer,
                        "boolean" => FieldType::Boolean,
                        "date" => FieldType::Date,
                        _ => {
                            // TODO; make this an error
                            warn!("unimplemented type name! {}", type_name);
                            FieldType::default()
                        }
                    };
                }
                "index" => {
                    let index = value.as_string().unwrap();
                    if index == "not_analyzed" {
                        field_mapping.analyzer = Analyzer::None;
                    } else {
                        // TODO: Implement other variants and make this an error
                        warn!("unimplemented index setting! {}", index);
                    }
                }
                "index_analyzer" => {
                    if let Some(ref s) = value.as_string() {
                        if s == &"edgengram_analyzer" {
                            field_mapping.analyzer = Analyzer::EdgeNGram;
                        }
                    }
                }
                "boost" => {
                    field_mapping.boost = value.as_f64().unwrap();
                }
                "store" => {
                    field_mapping.is_stored = parse_boolean(value);
                }
                "include_in_all" => {
                    field_mapping.is_in_all = parse_boolean(value);
                }
                _ => warn!("unimplemented field mapping key! {}", key),
            }

        }

        field_mapping
    }
}


#[derive(Debug)]
pub struct MappingRegistry {
    mappings: HashMap<String, Mapping>,
}


impl MappingRegistry {
    pub fn new() -> MappingRegistry {
        MappingRegistry {
            mappings: HashMap::new(),
        }
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldMapping> {
        for mapping in self.mappings.values() {
            if let Some(ref field_mapping) = mapping.fields.get(name) {
                return Some(field_mapping);
            }
        }

        None
    }
}


impl Deref for MappingRegistry {
    type Target = HashMap<String, Mapping>;

    fn deref(&self) -> &HashMap<String, Mapping> {
        &self.mappings
    }
}


impl DerefMut for MappingRegistry {
    fn deref_mut(&mut self) -> &mut HashMap<String, Mapping> {
        &mut self.mappings
    }
}
