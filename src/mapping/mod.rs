pub mod build;
pub mod parse;

use std::collections::{HashMap, BTreeMap};

use serde::{Serialize, Serializer};
use serde_json;
//use serde_json::value::ToJson;
use chrono::{DateTime, Utc};
use kite::{Term, Token};
use kite::term_vector::TermVector;
use kite::document::FieldValue;
use kite::similarity::SimilarityModel;
use kite::schema::FieldRef;

use analysis::AnalyzerSpec;
use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;


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


#[derive(Debug, Clone, Copy, PartialEq)]
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


impl ToString for FieldType {
    fn to_string(&self) -> String {
        match *self {
            FieldType::String => "string".to_string(),
            FieldType::Integer => "integer".to_string(),
            FieldType::Boolean => "boolean".to_string(),
            FieldType::Date => "date".to_string(),
        }
    }
}


#[derive(Debug, Clone, PartialEq)]
pub struct FieldSearchOptions {
    pub analyzer: Option<AnalyzerSpec>,
    pub similarity_model: SimilarityModel,
}


impl Default for FieldSearchOptions {
    fn default() -> FieldSearchOptions {
        FieldSearchOptions {
            analyzer: Some(get_standard_analyzer()),
            similarity_model: SimilarityModel::Bm25 {
                k1: 1.2,
                b: 0.75,
            },
        }
    }
}


#[derive(Debug)]
pub struct FieldValueError;


#[derive(Debug, PartialEq)]
pub struct FieldMapping {
    pub data_type: FieldType,
    pub index_ref: Option<FieldRef>,
    pub is_indexed: bool,
    pub is_stored: bool,
    pub is_in_all: bool,
    boost: f64,
    index_analyzer: Option<AnalyzerSpec>,
    search_analyzer: Option<AnalyzerSpec>,
}


impl Default for FieldMapping {
    fn default() -> FieldMapping {
        FieldMapping {
            data_type: FieldType::default(),
            index_ref: None,
            is_indexed: true,
            is_stored: false,
            is_in_all: true,
            boost: 1.0f64,
            index_analyzer: None,
            search_analyzer: None,
        }
    }
}


impl Serialize for FieldMapping {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let index = match (self.is_indexed, &self.index_analyzer) {
            (false, &None) => "no",
            (true, &None) => "not_analyzed",
            _ => {
                if self.data_type == FieldType::String {
                    "analyzed"
                } else {
                    "not_analyzed"
                }
            }
        };

        let json = json!({
            "type": self.data_type.to_string(),
            "index": index,
            "store": self.is_stored,
            // TODO
            // "index_analyzer"
            // "search_analyzer"
            "boost": self.boost,
            "include_in_all": self.is_in_all
        });

        json.serialize(serializer)
    }
}


impl FieldMapping {
    pub fn index_analyzer(&self) -> Option<&AnalyzerSpec> {
        if let Some(ref index_analyzer) = self.index_analyzer {
            Some(index_analyzer)
        } else {
            None
        }
    }

    pub fn search_analyzer(&self) -> Option<&AnalyzerSpec> {
        if let Some(ref search_analyzer) = self.search_analyzer {
            Some(search_analyzer)
        } else {
            None
        }
    }

    pub fn get_search_options(&self) -> FieldSearchOptions {
        FieldSearchOptions {
            analyzer: self.search_analyzer().cloned(),
            .. FieldSearchOptions::default()
        }
    }

    pub fn process_value_for_index(&self, value: &serde_json::Value) -> Result<Option<TermVector>, FieldValueError> {
        if *value == serde_json::Value::Null {
            return Ok(None);
        }

        match self.data_type {
            FieldType::String => {
                match *value {
                    serde_json::Value::String(ref string) => {
                        // Analyze string
                        let tokens = match self.index_analyzer() {
                            Some(index_analyzer) => {
                                let token_stream = index_analyzer.initialise(string);
                                token_stream.collect::<Vec<Token>>().into()
                            }
                            None => {
                                vec![
                                    Token {term: Term::from_string(string), position: 1}
                                ].into()
                            }
                        };
                        Ok(Some(tokens))
                    }
                    serde_json::Value::Number(ref num) => self.process_value_for_index(&serde_json::Value::String(num.to_string())),
                    serde_json::Value::Array(ref array) => {
                        // Process each array item and merge tokens together
                        let mut tokens = Vec::new();
                        let mut last_token_position = 0;

                        for item in array {
                            match *item {
                                serde_json::Value::String(ref string) => {
                                    if let Some(next_tokens) = self.process_value_for_index(&serde_json::Value::String(string.clone()))? {
                                        let mut next_tokens: Vec<Token> = next_tokens.into();

                                        // Increment token positions so they don't overlap with previous values
                                        for token in next_tokens.iter_mut() {
                                            token.position += last_token_position;
                                        }

                                        // Update last_token_position
                                        if let Some(token) = next_tokens.last() {
                                            last_token_position = token.position;
                                        }

                                        // Merge
                                        tokens.reserve(next_tokens.len());
                                        for token in next_tokens {
                                            tokens.push(token);
                                        }
                                    }
                                }
                                serde_json::Value::Null => {}
                                _ => {
                                    return Err(FieldValueError);
                                }
                            }
                        }

                        Ok(Some(tokens.into()))
                    }
                    _ => Err(FieldValueError),
                }
            }
            FieldType::Integer => {
                match *value {
                    serde_json::Value::Number(ref num) => {
                        match num.as_i64() {
                            Some(num) => Ok(Some(vec![Token{term: Term::from_integer(num), position: 1}].into())),
                            None => Err(FieldValueError),
                        }
                    }
                    _ => Err(FieldValueError),
                }
            }
            FieldType::Boolean => Ok(Some(vec![Token{term: Term::from_boolean(parse_boolean(&value)), position: 1}].into())),
            FieldType::Date => {
                match *value {
                    serde_json::Value::String(ref string) => {
                        let date_parsed = match string.parse::<DateTime<Utc>>() {
                            Ok(date_parsed) => date_parsed,
                            Err(_) => {
                                // TODO: Handle this properly
                                return Err(FieldValueError);
                            }
                        };

                        Ok(Some(vec![Token{term: Term::from_datetime(&date_parsed), position: 1}].into()))
                    }
                    serde_json::Value::Number(_) => {
                        // TODO needs to be interpreted as milliseconds since epoch
                        // This would really help: https://github.com/lifthrasiir/rust-chrono/issues/74
                        Err(FieldValueError)
                    }
                    _ => Err(FieldValueError),
                }
            }
        }
    }

    pub fn process_value_for_store(&self, value: &serde_json::Value) -> Result<Option<FieldValue>, FieldValueError> {
        if *value == serde_json::Value::Null {
            return Ok(None);
        }

        match self.data_type {
            FieldType::String => {
                match *value {
                    serde_json::Value::String(ref string) => {
                        Ok(Some(FieldValue::String(string.clone())))
                    }
                    serde_json::Value::Number(ref num) => self.process_value_for_store(&serde_json::Value::String(num.to_string())),
                    serde_json::Value::Array(ref array) => {
                        // Pack any strings into a vec, ignore nulls. Quit if we see anything else
                        let mut strings = Vec::new();

                        for item in array {
                            match *item {
                                serde_json::Value::String(ref string) => strings.push(string.clone()),
                                serde_json::Value::Null => {}
                                _ => {
                                    return Err(FieldValueError);
                                }
                            }
                        }

                        self.process_value_for_store(&serde_json::Value::String(strings.join(" ")))
                    }
                    _ => Err(FieldValueError),
                }
            }
            FieldType::Integer => {
                match *value {
                    serde_json::Value::Number(ref num) => {
                        match num.as_i64() {
                            Some(num) => Ok(Some(FieldValue::Integer(num))),
                            None => Err(FieldValueError)
                        }
                    }
                    _ => Err(FieldValueError),
                }
            }
            FieldType::Boolean => Ok(Some(FieldValue::Boolean(parse_boolean(&value)))),
            FieldType::Date => {
                match *value {
                    serde_json::Value::String(ref string) => {
                        let date_parsed = match string.parse::<DateTime<Utc>>() {
                            Ok(date_parsed) => date_parsed,
                            Err(_) => {
                                // TODO: Handle this properly
                                return Err(FieldValueError);
                            }
                        };

                        Ok(Some(FieldValue::DateTime(date_parsed)))
                    }
                    serde_json::Value::Number(_) => {
                        // TODO needs to be interpreted as milliseconds since epoch
                        // This would really help: https://github.com/lifthrasiir/rust-chrono/issues/74
                        Err(FieldValueError)
                    }
                    _ => Err(FieldValueError)
                }
            }
        }
    }
}


#[derive(Debug, PartialEq)]
pub struct NestedMapping {
    pub properties: HashMap<String, MappingProperty>,
}


impl Serialize for NestedMapping {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut properties_json = BTreeMap::new();

        // TODO: Exclude "_all" field
        for (name, prop) in self.properties.iter() {
            properties_json.insert(name.to_string(), serde_json::to_value(&prop).unwrap());
        }

        let json = json!({
            "type": "nested",
            "properties": properties_json,
        });

        json.serialize(serializer)
    }
}


#[derive(Debug, PartialEq)]
pub enum MappingProperty {
    Field(FieldMapping),
    NestedMapping(Box<NestedMapping>),
}


impl Serialize for MappingProperty {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match *self {
            MappingProperty::Field(ref field) => field.serialize(serializer),
            MappingProperty::NestedMapping(ref mapping) => mapping.serialize(serializer),
        }
    }
}


#[derive(Debug, PartialEq)]
pub struct Mapping {
    pub properties: HashMap<String, MappingProperty>,
}


impl Serialize for Mapping {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut properties_json = BTreeMap::new();

        // TODO: Exclude "_all" field
        for (name, prop) in self.properties.iter() {
            properties_json.insert(name.to_string(), serde_json::to_value(&prop).unwrap());
        }

        let json = json!({
            "properties": properties_json,
        });

        json.serialize(serializer)
    }
}


fn parse_boolean(json: &serde_json::Value) -> bool {
    match *json {
        serde_json::Value::Bool(val) => val,
        serde_json::Value::String(ref s) => {
            match s.as_ref() {
                "yes" => true,
                "no" => false,
                _ => {
                    // warn!("bad boolean value {:?}", s);
                    false
                }
           }
        }
        _ => {
            // TODO: Raise error
            // warn!("bad boolean value {:?}", json);
            false
        }
    }
}
