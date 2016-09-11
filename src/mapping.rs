use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use rustc_serialize::json::Json;
use chrono::{DateTime, UTC};
use abra::{Term, Token};
use abra::analysis::AnalyzerSpec;
use abra::analysis::registry::AnalyzerRegistry;
use abra::analysis::tokenizers::TokenizerSpec;
use abra::analysis::filters::FilterSpec;
use abra::similarity::SimilarityModel;
use abra::schema::FieldRef;


// TEMPORARY
fn get_standard_analyzer() -> AnalyzerSpec {
    AnalyzerSpec {
        tokenizer: TokenizerSpec::Standard,
        filters: vec![
            FilterSpec::Lowercase,
            FilterSpec::ASCIIFolding,
        ]
    }
}


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


#[derive(Debug, Clone, PartialEq)]
pub struct FieldSearchOptions {
    pub analyzer: AnalyzerSpec,
    pub similarity_model: SimilarityModel,
}


impl Default for FieldSearchOptions {
    fn default() -> FieldSearchOptions {
        FieldSearchOptions {
            analyzer: get_standard_analyzer(),
            similarity_model: SimilarityModel::Bm25 {
                k1: 1.2,
                b: 0.75,
            },
        }
    }
}


#[derive(Debug)]
pub struct FieldMapping {
    pub data_type: FieldType,
    pub index_ref: Option<FieldRef>,
    is_stored: bool,
    pub is_in_all: bool,
    boost: f64,
    base_analyzer: AnalyzerSpec,
    index_analyzer: Option<AnalyzerSpec>,
    search_analyzer: Option<AnalyzerSpec>,
}


impl Default for FieldMapping {
    fn default() -> FieldMapping {
        FieldMapping {
            data_type: FieldType::default(),
            index_ref: None,
            is_stored: false,
            is_in_all: true,
            boost: 1.0f64,
            base_analyzer: get_standard_analyzer(),
            index_analyzer: None,
            search_analyzer: None,
        }
    }
}


impl FieldMapping {
    pub fn index_analyzer(&self) -> &AnalyzerSpec {
        if let Some(ref index_analyzer) = self.index_analyzer {
            index_analyzer
        } else {
            &self.base_analyzer
        }
    }

    pub fn search_analyzer(&self) -> &AnalyzerSpec {
        if let Some(ref search_analyzer) = self.search_analyzer {
            search_analyzer
        } else {
            &self.base_analyzer
        }
    }

    pub fn get_search_options(&self) -> FieldSearchOptions {
        FieldSearchOptions {
            analyzer: self.search_analyzer().clone(),
            .. FieldSearchOptions::default()
        }
    }

    fn process_value(&self, analyzer: Option<&AnalyzerSpec>, value: Json) -> Option<Vec<Token>> {
        if value == Json::Null {
            return None;
        }

        match self.data_type {
            FieldType::String => {
                match value {
                    Json::String(string) => {
                        // Analyze string
                        match analyzer {
                            Some(ref analyzer) => {
                                let tokens = analyzer.initialise(&string);
                                Some(tokens.collect::<Vec<Token>>())
                            }
                            None => Some(vec![Token{term: Term::String(string), position: 1}]),
                        }
                    }
                    Json::I64(num) => self.process_value(analyzer, Json::String(num.to_string())),
                    Json::U64(num) => self.process_value(analyzer, Json::String(num.to_string())),
                    Json::F64(num) => self.process_value(analyzer, Json::String(num.to_string())),
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

                        self.process_value(analyzer, Json::String(strings.join(" ")))
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
                            Err(_) => {
                                // TODO: Handle this properly
                                return None;
                            }
                        };

                        Some(vec![Token{term: Term::DateTime(date_parsed), position: 1}])
                    }
                    Json::U64(_) => {
                        // TODO needs to be interpreted as milliseconds since epoch
                        // This would really help: https://github.com/lifthrasiir/rust-chrono/issues/74
                        None
                    }
                    _ => None
                }
            }
        }
    }

    pub fn process_value_for_index(&self, value: Json) -> Option<Vec<Token>> {
        self.process_value(Some(self.index_analyzer()), value)
    }

    pub fn process_value_for_query(&self, value: Json) -> Option<Vec<Token>> {
        self.process_value(Some(self.search_analyzer()), value)
    }
}


#[derive(Debug)]
pub struct Mapping {
    pub fields: HashMap<String, FieldMapping>,
}

impl Mapping {
    pub fn from_json(analyzers: &AnalyzerRegistry, json: &Json) -> Mapping {
        let json = json.as_object().unwrap();
        let properties_json = json.get("properties").unwrap().as_object().unwrap();

        // Parse fields
        let mut fields = HashMap::new();
        for (field_name, field_mapping_json) in properties_json.iter() {
            fields.insert(field_name.clone(),
                          FieldMapping::from_json(analyzers, field_mapping_json));
        }

        // Insert _all field
        if !fields.contains_key("_all") {
            // TODO: Support disabling the _all field
            fields.insert("_all".to_string(), FieldMapping {
                data_type: FieldType::String,
                is_stored: false,
                is_in_all: false,
                .. FieldMapping::default()
            });
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
    pub fn from_json(analyzers: &AnalyzerRegistry, json: &Json) -> FieldMapping {
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
                        field_mapping.index_analyzer = None;
                        field_mapping.search_analyzer = None;
                    } else {
                        // TODO: Implement other variants and make this an error
                        warn!("unimplemented index setting! {}", index);
                    }
                }
                "analyzer" => {
                    if let Some(ref s) = value.as_string() {
                        match analyzers.get(*s) {
                            Some(analyzer) => {
                                field_mapping.base_analyzer = analyzer.clone();
                            }
                            None => warn!("unknown analyzer! {}", s)
                        }
                    }
                }
                "index_analyzer" => {
                    if let Some(ref s) = value.as_string() {
                        match analyzers.get(*s) {
                            Some(analyzer) => {
                                field_mapping.index_analyzer = Some(analyzer.clone());
                            }
                            None => warn!("unknown analyzer! {}", s)
                        }
                    }
                }
                "search_analyzer" => {
                    if let Some(ref s) = value.as_string() {
                        match analyzers.get(*s) {
                            Some(analyzer) => {
                                field_mapping.search_analyzer = Some(analyzer.clone());
                            }
                            None => warn!("unknown analyzer! {}", s)
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
