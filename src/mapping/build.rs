use std::collections::HashMap;

use mapping::FieldType;


#[derive(Debug, PartialEq)]
pub struct FieldMappingBuilder {
    pub field_type: FieldType,
    pub is_indexed: bool,
    pub is_analyzed: bool,
    pub is_stored: bool,
    pub is_in_all: bool,
    pub boost: f64,
    pub base_analyzer: String,
    pub index_analyzer: Option<String>,
    pub search_analyzer: Option<String>
}


impl Default for FieldMappingBuilder {
    fn default() -> FieldMappingBuilder {
        FieldMappingBuilder {
            field_type: FieldType::String,
            is_indexed: true,
            is_analyzed: true,
            is_stored: false,
            is_in_all: true,
            boost: 1.0f64,
            base_analyzer: "default".to_string(),
            index_analyzer: None,
            search_analyzer: None,
        }
    }
}


#[derive(Debug, PartialEq)]
pub struct MappingBuilder {
    pub properties: HashMap<String, FieldMappingBuilder>,
}
