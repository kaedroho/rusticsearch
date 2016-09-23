use std::collections::HashMap;

use rustc_serialize::json::Json;

use mapping::{Mapping, FieldType};


#[derive(Debug, PartialEq)]
pub struct FieldMappingBuilder {
    field_type: FieldType,
    is_indexed: bool,
    is_analyzed: bool,
    is_stored: bool,
    is_in_all: bool,
    boost: f64,
    base_analyzer: String,
    index_analyzer: Option<String>,
    search_analyzer: Option<String>
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
    properties: HashMap<String, FieldMappingBuilder>,
}


#[derive(Debug, PartialEq)]
pub enum MappingParseError {
    ExpectedObject,
    ExpectedString,
    ExpectedBoolean,
    ExpectedKey(String),

    // Field type
    UnrecognisedFieldType(String),

    // "index" setting
    IndexAnalyzedOnlyAllowedOnStringType,
    UnrecognisedIndexSetting(String),

    // "analyzer" settings
    AnalyzersOnlyAllowedOnStringType,
    AnalyzersOnlyAllowedOnAnalyzedFields,
}


fn parse_boolean(json: &Json) -> Result<bool, MappingParseError> {
    match *json {
        Json::Boolean(val) => Ok(val),
        Json::String(ref s) => {
            match s.as_ref() {
                "yes" => Ok(true),
                "no" => Ok(false),
                _ => Err(MappingParseError::ExpectedBoolean)
            }
        }
        _ => Err(MappingParseError::ExpectedBoolean)
    }
}


fn parse_field_type(field_type_str: &str) -> Result<FieldType, MappingParseError> {
    match field_type_str {
        "string" => Ok(FieldType::String),
        "integer" => Ok(FieldType::Integer),
        "boolean" => Ok(FieldType::Boolean),
        "date" => Ok(FieldType::Date),
        _ => Err(MappingParseError::UnrecognisedFieldType(field_type_str.to_string())),
    }
}


fn parse_field(json: &Json) -> Result<FieldMappingBuilder, MappingParseError> {
    let field_object = try!(json.as_object().ok_or(MappingParseError::ExpectedObject));
    let mut mapping_builder = FieldMappingBuilder::default();

    // Field type
    let field_type_json = try!(field_object.get("type").ok_or(MappingParseError::ExpectedKey("type".to_string())));
    let field_type_str = try!(field_type_json.as_string().ok_or(MappingParseError::ExpectedString));
    mapping_builder.field_type = try!(parse_field_type(field_type_str));

    // "index" setting
    if let Some(index_json) = field_object.get("index") {
        let index_str = try!(index_json.as_string().ok_or(MappingParseError::ExpectedString));

        match index_str {
            "no" => {
                mapping_builder.is_indexed = false;
                mapping_builder.is_analyzed = false;
            }
            "not_analyzed" => {
                mapping_builder.is_indexed = true;
                mapping_builder.is_analyzed = false;
            }
            "analyzed" => {
                mapping_builder.is_indexed = true;
                mapping_builder.is_analyzed = true;

                // Not valid for non-string fields
                if mapping_builder.field_type != FieldType::String {
                    return Err(MappingParseError::IndexAnalyzedOnlyAllowedOnStringType);
                }
            }
            _ => {
                return Err(MappingParseError::UnrecognisedIndexSetting(index_str.to_string()));
            }
        }
    }

    // "store" setting
    if let Some(store_json) = field_object.get("store") {
        mapping_builder.is_stored = try!(parse_boolean(store_json));
    }

    // Analyzers
    if let Some(analyzer_json) = field_object.get("analyzer") {
        let analyzer_str = try!(analyzer_json.as_string().ok_or(MappingParseError::ExpectedString));
        mapping_builder.base_analyzer = analyzer_str.to_string();

        if mapping_builder.field_type != FieldType::String {
            return Err(MappingParseError::AnalyzersOnlyAllowedOnStringType);
        }

        if !mapping_builder.is_analyzed {
            return Err(MappingParseError::AnalyzersOnlyAllowedOnAnalyzedFields);
        }
    }

    if let Some(index_analyzer_json) = field_object.get("index_analyzer") {
        let index_analyzer_str = try!(index_analyzer_json.as_string().ok_or(MappingParseError::ExpectedString));
        mapping_builder.index_analyzer = Some(index_analyzer_str.to_string());

        if mapping_builder.field_type != FieldType::String {
            return Err(MappingParseError::AnalyzersOnlyAllowedOnStringType);
        }

        if !mapping_builder.is_analyzed {
            return Err(MappingParseError::AnalyzersOnlyAllowedOnAnalyzedFields);
        }
    }

    if let Some(search_analyzer_json) = field_object.get("search_analyzer") {
        let search_analyzer_str = try!(search_analyzer_json.as_string().ok_or(MappingParseError::ExpectedString));
        mapping_builder.search_analyzer = Some(search_analyzer_str.to_string());

        if mapping_builder.field_type != FieldType::String {
            return Err(MappingParseError::AnalyzersOnlyAllowedOnStringType);
        }

        if !mapping_builder.is_analyzed {
            return Err(MappingParseError::AnalyzersOnlyAllowedOnAnalyzedFields);
        }
    }

    Ok(mapping_builder)
}


pub fn parse(json: &Json) -> Result<MappingBuilder, MappingParseError> {
    let mapping_object = try!(json.as_object().ok_or(MappingParseError::ExpectedObject));

    // Parse properties
    let properties_json = try!(mapping_object.get("properties").ok_or(MappingParseError::ExpectedKey("properties".to_string())));
    let properties_object = try!(properties_json.as_object().ok_or(MappingParseError::ExpectedObject));
    let mut properties = HashMap::new();

    for (field_name, field_json) in properties_object {
        let field = try!(parse_field(field_json));
        properties.insert(field_name.to_string(), field);
    }

    Ok(MappingBuilder {
        properties: properties,
    })
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use mapping::FieldType;

    use super::{FieldMappingBuilder, MappingBuilder, MappingParseError, parse, parse_field};

    #[test]
    fn test_parse() {
        let mapping = parse(&Json::from_str("
        {
            \"properties\": {
                \"myfield\": {
                    \"type\": \"string\"
                }
            }
        }
        ").unwrap());

        assert_eq!(mapping, Ok(MappingBuilder {
            properties: hashmap! {
                "myfield".to_string() => FieldMappingBuilder {
                    field_type: FieldType::String,
                    ..FieldMappingBuilder::default()
                }
            }
        }));
    }

    #[test]
    fn test_parse_empty() {
        let mapping = parse(&Json::from_str("
        {}
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedKey("properties".to_string())));
    }

    #[test]
    fn test_parse_bad_type() {
        // Array
        let mapping = parse(&Json::from_str("
        [\"foo\"]
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));

        // String
        let mapping = parse(&Json::from_str("
        \"foo\"
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));

        // Number
        let mapping = parse(&Json::from_str("
        123
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));
    }

    #[test]
    fn test_parse_empty_properties() {
        let mapping = parse(&Json::from_str("
        {
            \"properties\": {}
        }
        ").unwrap());

        assert_eq!(mapping, Ok(MappingBuilder {
            properties: hashmap! {},
        }));
    }

    #[test]
    fn test_parse_bad_type_properties() {
        // Array
        let mapping = parse(&Json::from_str("
        {
            \"properties\": [\"foo\"]
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));

        // String
        let mapping = parse(&Json::from_str("
        {
            \"properties\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));

        // Number
        let mapping = parse(&Json::from_str("
        {
            \"properties\": 123
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));
    }

    #[test]
    fn test_parse_field_types() {
        // String
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            ..FieldMappingBuilder::default()
        }));

        // Integer
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"integer\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::Integer,
            ..FieldMappingBuilder::default()
        }));

        // Boolean
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"boolean\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::Boolean,
            ..FieldMappingBuilder::default()
        }));

        // Date
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"date\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::Date,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_field_no_type() {
        let mapping = parse_field(&Json::from_str("
        {
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedKey("type".to_string())));
    }

    #[test]
    fn test_parse_field_unrecognised_type() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::UnrecognisedFieldType("foo".to_string())));
    }

    #[test]
    fn test_parse_field_type_not_string() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": 123
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedString));
    }

    #[test]
    fn test_parse_index_default() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_indexed: true,
            is_analyzed: true,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_index_no() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"index\": \"no\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_indexed: false,
            is_analyzed: false,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_index_not_analyzed() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"index\": \"not_analyzed\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_indexed: true,
            is_analyzed: false,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_index_analyzed() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"index\": \"analyzed\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_indexed: true,
            is_analyzed: true,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_index_analyzed_on_non_string_type() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"integer\",
            \"index\": \"analyzed\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::IndexAnalyzedOnlyAllowedOnStringType));
    }

    #[test]
    fn test_parse_index_unrecognised_value() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"index\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::UnrecognisedIndexSetting("foo".to_string())));
    }

    #[test]
    fn test_parse_store_default() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_stored: false,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_store_yes() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"store\": \"yes\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_stored: true,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_store_true() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"store\": true
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_stored: true,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_store_non_boolean() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"store\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedBoolean));
    }

    #[test]
    fn test_parse_analyzer_default() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: "default".to_string(),
            index_analyzer: None,
            search_analyzer: None,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_analyzer() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: "foo".to_string(),
            index_analyzer: None,
            search_analyzer: None,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_index_analyzer() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"index_analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: "default".to_string(),
            index_analyzer: Some("foo".to_string()),
            search_analyzer: None,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_search_analyzer() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"search_analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: "default".to_string(),
            index_analyzer: None,
            search_analyzer: Some("foo".to_string()),
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_analyzer_on_integer_field() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"integer\",
            \"analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::AnalyzersOnlyAllowedOnStringType));
    }

    #[test]
    fn test_parse_analyzer_on_non_analyzed_field() {
        let mapping = parse_field(&Json::from_str("
        {
            \"type\": \"string\",
            \"index\": \"not_analyzed\",
            \"analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::AnalyzersOnlyAllowedOnAnalyzedFields));
    }
}