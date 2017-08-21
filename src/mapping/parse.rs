use std::collections::{HashMap, BTreeSet};

use serde_json;

use mapping::FieldType;
use mapping::build::{MappingBuilder, MappingPropertyBuilder, FieldMappingBuilder, NestedMappingBuilder};


#[derive(Debug, PartialEq)]
pub enum FieldMappingParseError {
    ExpectedObject,
    ExpectedString,
    ExpectedBoolean,
    ExpectedNumber,
    ExpectedKey(String),
    UnrecognisedKeys(Vec<String>),

    // Field type
    UnrecognisedFieldType(String),

    // "index" setting
    IndexAnalyzedOnlyAllowedOnStringType,
    UnrecognisedIndexSetting(String),

    // "analyzer" settings
    AnalyzersOnlyAllowedOnStringType,
    AnalyzersOnlyAllowedOnAnalyzedFields,

    // "boost" setting
    BoostOnlyAllowedOnIndexedFields,
    BoostMustBePositive,
}


#[derive(Debug, PartialEq)]
pub enum MappingParseError {
    ExpectedObject,
    ExpectedString,
    ExpectedBoolean,
    ExpectedNumber,
    ExpectedKey(String),
    UnrecognisedKeys(Vec<String>),
    FieldMappingParseError(String, FieldMappingParseError),
    NestedMappingParseError(String, Box<MappingParseError>),
}


fn parse_boolean(json: &serde_json::Value) -> Result<bool, FieldMappingParseError> {
    match *json {
        serde_json::Value::Bool(val) => Ok(val),
        serde_json::Value::String(ref s) => {
            match s.as_ref() {
                "yes" => Ok(true),
                "no" => Ok(false),
                _ => Err(FieldMappingParseError::ExpectedBoolean)
            }
        }
        _ => Err(FieldMappingParseError::ExpectedBoolean)
    }
}


fn parse_float(json: &serde_json::Value) -> Result<f64, FieldMappingParseError> {
    match *json {
        serde_json::Value::Number(ref num) => {
            match num.as_f64() {
                Some(num) => Ok(num),
                None => Err(FieldMappingParseError::ExpectedNumber),
            }
        }
        _ => Err(FieldMappingParseError::ExpectedNumber)
    }
}


fn parse_field_type(field_type_str: &str) -> Result<FieldType, FieldMappingParseError> {
    match field_type_str {
        "string" => Ok(FieldType::String),
        "integer" => Ok(FieldType::Integer),
        "boolean" => Ok(FieldType::Boolean),
        "date" => Ok(FieldType::Date),
        _ => Err(FieldMappingParseError::UnrecognisedFieldType(field_type_str.to_string())),
    }
}


fn parse_field(json: &serde_json::Value) -> Result<FieldMappingBuilder, FieldMappingParseError> {
    let field_object = try!(json.as_object().ok_or(FieldMappingParseError::ExpectedObject));
    let mut mapping_builder = FieldMappingBuilder::default();

    // Check for unrecognised keys
    let provided_keys = field_object.keys().cloned().collect::<BTreeSet<String>>();
    let allowed_keys = btreeset![
        "type".to_string(),
        "index".to_string(),
        "store".to_string(),
        "analyzer".to_string(),
        "index_analyzer".to_string(),
        "search_analyzer".to_string(),
        "boost".to_string(),
        "include_in_all".to_string(),
    ];
    let unrecognised_keys = provided_keys.difference(&allowed_keys).cloned().collect::<Vec<String>>();

    if !unrecognised_keys.is_empty() {
        return Err(FieldMappingParseError::UnrecognisedKeys(unrecognised_keys));
    }

    // Field type
    let field_type_json = try!(field_object.get("type").ok_or(FieldMappingParseError::ExpectedKey("type".to_string())));
    let field_type_str = try!(field_type_json.as_str().ok_or(FieldMappingParseError::ExpectedString));
    mapping_builder.field_type = try!(parse_field_type(field_type_str));

    // Non-string fields cannot be analyzed
    if mapping_builder.field_type != FieldType::String {
        mapping_builder.is_analyzed = false;
    }

    // "index" setting
    if let Some(index_json) = field_object.get("index") {
        let index_str = try!(index_json.as_str().ok_or(FieldMappingParseError::ExpectedString));

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
                    return Err(FieldMappingParseError::IndexAnalyzedOnlyAllowedOnStringType);
                }
            }
            _ => {
                return Err(FieldMappingParseError::UnrecognisedIndexSetting(index_str.to_string()));
            }
        }
    }

    // "store" setting
    if let Some(store_json) = field_object.get("store") {
        mapping_builder.is_stored = try!(parse_boolean(store_json));
    }

    // Analyzers
    if let Some(analyzer_json) = field_object.get("analyzer") {
        let analyzer_str = try!(analyzer_json.as_str().ok_or(FieldMappingParseError::ExpectedString));
        mapping_builder.base_analyzer = Some(analyzer_str.to_string());

        if mapping_builder.field_type != FieldType::String {
            return Err(FieldMappingParseError::AnalyzersOnlyAllowedOnStringType);
        }

        if !mapping_builder.is_analyzed {
            return Err(FieldMappingParseError::AnalyzersOnlyAllowedOnAnalyzedFields);
        }
    }

    if let Some(index_analyzer_json) = field_object.get("index_analyzer") {
        let index_analyzer_str = try!(index_analyzer_json.as_str().ok_or(FieldMappingParseError::ExpectedString));
        mapping_builder.index_analyzer = Some(index_analyzer_str.to_string());

        if mapping_builder.field_type != FieldType::String {
            return Err(FieldMappingParseError::AnalyzersOnlyAllowedOnStringType);
        }

        if !mapping_builder.is_analyzed {
            return Err(FieldMappingParseError::AnalyzersOnlyAllowedOnAnalyzedFields);
        }
    }

    if let Some(search_analyzer_json) = field_object.get("search_analyzer") {
        let search_analyzer_str = try!(search_analyzer_json.as_str().ok_or(FieldMappingParseError::ExpectedString));
        mapping_builder.search_analyzer = Some(search_analyzer_str.to_string());

        if mapping_builder.field_type != FieldType::String {
            return Err(FieldMappingParseError::AnalyzersOnlyAllowedOnStringType);
        }

        if !mapping_builder.is_analyzed {
            return Err(FieldMappingParseError::AnalyzersOnlyAllowedOnAnalyzedFields);
        }
    }

    // Boost
    if let Some(boost_json) = field_object.get("boost") {
        let boost_num = try!(parse_float(boost_json));
        mapping_builder.boost = boost_num;

        if !mapping_builder.is_indexed {
            return Err(FieldMappingParseError::BoostOnlyAllowedOnIndexedFields);
        }

        if boost_num < 0.0f64 {
            return Err(FieldMappingParseError::BoostMustBePositive);
        }
    }

    // "include_in_all" setting
    if let Some(include_in_all_json) = field_object.get("include_in_all") {
        let include_in_all = try!(parse_boolean(include_in_all_json));
        mapping_builder.is_in_all = include_in_all;
    }

    Ok(mapping_builder)
}


fn parse_nested_mapping(json: &serde_json::Value) -> Result<NestedMappingBuilder, MappingParseError> {
    let mapping_object = try!(json.as_object().ok_or(MappingParseError::ExpectedObject));

    // Check for unrecognised keys
    let provided_keys = mapping_object.keys().cloned().collect::<BTreeSet<String>>();
    let allowed_keys = btreeset![
        "type".to_string(),
        "properties".to_string(),
    ];
    let unrecognised_keys = provided_keys.difference(&allowed_keys).cloned().collect::<Vec<String>>();

    if !unrecognised_keys.is_empty() {
        return Err(MappingParseError::UnrecognisedKeys(unrecognised_keys));
    }

    // Parse properties
    let properties_json = try!(mapping_object.get("properties").ok_or(MappingParseError::ExpectedKey("properties".to_string())));
    let properties_object = try!(properties_json.as_object().ok_or(MappingParseError::ExpectedObject));
    let mut properties = HashMap::new();

    for (prop_name, prop_json) in properties_object {
        let prop_object = try!(prop_json.as_object().ok_or(MappingParseError::FieldMappingParseError(prop_name.to_string(), FieldMappingParseError::ExpectedObject)));

        if prop_object.get("type") == Some(&serde_json::Value::String("nested".to_string())) {
            // Property is a nested mapping
            match parse_nested_mapping(prop_json) {
                Ok(mapping) => {
                    properties.insert(prop_name.to_string(), MappingPropertyBuilder::NestedMapping(Box::new(mapping)));
                }
                Err(e) => {
                    return Err(MappingParseError::NestedMappingParseError(prop_name.to_string(), Box::new(e)));
                }
            }
        } else {
            // Property is a field (or maybe invalid, which is handled by parse_field)
            match parse_field(prop_json) {
                Ok(field) => {
                    properties.insert(prop_name.to_string(), MappingPropertyBuilder::Field(field));
                }
                Err(e) => {
                    return Err(MappingParseError::FieldMappingParseError(prop_name.to_string(), e));
                }
            }
        }
    }

    Ok(NestedMappingBuilder {
        properties: properties,
    })
}


pub fn parse(json: &serde_json::Value) -> Result<MappingBuilder, MappingParseError> {
    let mapping_object = try!(json.as_object().ok_or(MappingParseError::ExpectedObject));

    // Check for unrecognised keys
    let provided_keys = mapping_object.keys().cloned().collect::<BTreeSet<String>>();
    let allowed_keys = btreeset![
        "properties".to_string(),
    ];
    let unrecognised_keys = provided_keys.difference(&allowed_keys).cloned().collect::<Vec<String>>();

    if !unrecognised_keys.is_empty() {
        return Err(MappingParseError::UnrecognisedKeys(unrecognised_keys));
    }

    // Parse properties
    let properties_json = try!(mapping_object.get("properties").ok_or(MappingParseError::ExpectedKey("properties".to_string())));
    let properties_object = try!(properties_json.as_object().ok_or(MappingParseError::ExpectedObject));
    let mut properties = HashMap::new();

    for (prop_name, prop_json) in properties_object {
        let prop_object = try!(prop_json.as_object().ok_or(MappingParseError::FieldMappingParseError(prop_name.to_string(), FieldMappingParseError::ExpectedObject)));

        if prop_object.get("type") == Some(&serde_json::Value::String("nested".to_string())) {
            // Property is a nested mapping
            match parse_nested_mapping(prop_json) {
                Ok(mapping) => {
                    properties.insert(prop_name.to_string(), MappingPropertyBuilder::NestedMapping(Box::new(mapping)));
                }
                Err(e) => {
                    return Err(MappingParseError::NestedMappingParseError(prop_name.to_string(), Box::new(e)));
                }
            }
        } else {
            // Property is a field (or maybe invalid, which is handled by parse_field)
            match parse_field(prop_json) {
                Ok(field) => {
                    properties.insert(prop_name.to_string(), MappingPropertyBuilder::Field(field));
                }
                Err(e) => {
                    return Err(MappingParseError::FieldMappingParseError(prop_name.to_string(), e));
                }
            }
        }
    }

    Ok(MappingBuilder {
        properties: properties,
    })
}


#[cfg(test)]
mod tests {
    use serde_json;

    use mapping::FieldType;
    use mapping::build::{FieldMappingBuilder, NestedMappingBuilder, MappingPropertyBuilder, MappingBuilder};

    use super::{MappingParseError, FieldMappingParseError, parse, parse_field};

    #[test]
    fn test_parse() {
        let mapping = parse(&serde_json::from_str("
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
                "myfield".to_string() => MappingPropertyBuilder::Field(
                    FieldMappingBuilder {
                        field_type: FieldType::String,
                        ..FieldMappingBuilder::default()
                    }
                )
            }
        }));
    }

    #[test]
    fn test_parse_nested() {
        let mapping = parse(&serde_json::from_str("
        {
            \"properties\": {
                \"myfield\": {
                    \"type\": \"nested\",
                    \"properties\": {
                        \"foo\": {
                            \"type\": \"string\"
                        }
                    }
                }
            }
        }
        ").unwrap());

        assert_eq!(mapping, Ok(MappingBuilder {
            properties: hashmap! {
                "myfield".to_string() => MappingPropertyBuilder::NestedMapping(Box::new(
                    NestedMappingBuilder {
                        properties: hashmap! {
                            "foo".to_string() => MappingPropertyBuilder::Field(
                                FieldMappingBuilder {
                                    field_type: FieldType::String,
                                    ..FieldMappingBuilder::default()
                                }
                            )
                        }
                    }
                ))
            }
        }));
    }

    #[test]
    fn test_parse_nested_nested() {
        let mapping = parse(&serde_json::from_str("
        {
            \"properties\": {
                \"myfield\": {
                    \"type\": \"nested\",
                    \"properties\": {
                        \"mynestedfield\": {
                            \"type\": \"nested\",
                            \"properties\": {
                                \"foo\": {
                                    \"type\": \"string\"
                                }
                            }
                        }
                    }
                }
            }
        }
        ").unwrap());

        assert_eq!(mapping, Ok(MappingBuilder {
            properties: hashmap! {
                "myfield".to_string() => MappingPropertyBuilder::NestedMapping(Box::new(
                    NestedMappingBuilder {
                        properties: hashmap! {
                            "mynestedfield".to_string() => MappingPropertyBuilder::NestedMapping(Box::new(
                                NestedMappingBuilder {
                                    properties: hashmap! {
                                        "foo".to_string() => MappingPropertyBuilder::Field(
                                            FieldMappingBuilder {
                                                field_type: FieldType::String,
                                                ..FieldMappingBuilder::default()
                                            }
                                        )
                                    }
                                }
                            ))
                        }
                    }
                ))
            }
        }));
    }

    #[test]
    fn test_parse_field_error() {
        let mapping = parse(&serde_json::from_str("
        {
            \"properties\": {
                \"myfield\": {
                }
            }
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::FieldMappingParseError("myfield".to_string(), FieldMappingParseError::ExpectedKey("type".to_string()))));
    }

    #[test]
    fn test_parse_empty() {
        let mapping = parse(&serde_json::from_str("
        {}
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedKey("properties".to_string())));
    }

    #[test]
    fn test_parse_bad_type() {
        // Array
        let mapping = parse(&serde_json::from_str("
        [\"foo\"]
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));

        // String
        let mapping = parse(&serde_json::from_str("
        \"foo\"
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));

        // Number
        let mapping = parse(&serde_json::from_str("
        123
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));
    }

    #[test]
    fn test_parse_empty_properties() {
        let mapping = parse(&serde_json::from_str("
        {
            \"properties\": {}
        }
        ").unwrap());

        assert_eq!(mapping, Ok(MappingBuilder {
            properties: hashmap! {},
        }));
    }

    #[test]
    fn test_parse_unrecognised_key() {
        let mapping = parse(&serde_json::from_str("
        {
            \"properties\": {},
            \"foo\": \"bar\",
            \"baz\": \"quux\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::UnrecognisedKeys(vec!["baz".to_string(), "foo".to_string()])));
    }

    #[test]
    fn test_parse_bad_type_properties() {
        // Array
        let mapping = parse(&serde_json::from_str("
        {
            \"properties\": [\"foo\"]
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));

        // String
        let mapping = parse(&serde_json::from_str("
        {
            \"properties\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));

        // Number
        let mapping = parse(&serde_json::from_str("
        {
            \"properties\": 123
        }
        ").unwrap());

        assert_eq!(mapping, Err(MappingParseError::ExpectedObject));
    }

    #[test]
    fn test_parse_field_types() {
        // String
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_analyzed: true,
            ..FieldMappingBuilder::default()
        }));

        // Integer
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"integer\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::Integer,
            is_analyzed: false,
            ..FieldMappingBuilder::default()
        }));

        // Boolean
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"boolean\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::Boolean,
            is_analyzed: false,
            ..FieldMappingBuilder::default()
        }));

        // Date
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"date\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::Date,
            is_analyzed: false,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_field_no_type() {
        let mapping = parse_field(&serde_json::from_str("
        {
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::ExpectedKey("type".to_string())));
    }

    #[test]
    fn test_parse_field_unrecognised_type() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::UnrecognisedFieldType("foo".to_string())));
    }

    #[test]
    fn test_parse_field_type_not_string() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": 123
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::ExpectedString));
    }

    #[test]
    fn test_parse_field_unrecognised_key() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"foo\": \"bar\",
            \"baz\": \"quux\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::UnrecognisedKeys(vec!["baz".to_string(), "foo".to_string()])));
    }

    #[test]
    fn test_parse_index_default() {
        let mapping = parse_field(&serde_json::from_str("
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
        let mapping = parse_field(&serde_json::from_str("
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
        let mapping = parse_field(&serde_json::from_str("
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
        let mapping = parse_field(&serde_json::from_str("
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
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"integer\",
            \"index\": \"analyzed\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::IndexAnalyzedOnlyAllowedOnStringType));
    }

    #[test]
    fn test_parse_index_unrecognised_value() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"index\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::UnrecognisedIndexSetting("foo".to_string())));
    }

    #[test]
    fn test_parse_store_default() {
        let mapping = parse_field(&serde_json::from_str("
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
        let mapping = parse_field(&serde_json::from_str("
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
        let mapping = parse_field(&serde_json::from_str("
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
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"store\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::ExpectedBoolean));
    }

    #[test]
    fn test_parse_analyzer_default() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: None,
            index_analyzer: None,
            search_analyzer: None,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_analyzer() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: Some("foo".to_string()),
            index_analyzer: None,
            search_analyzer: None,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_index_analyzer() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"index_analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: None,
            index_analyzer: Some("foo".to_string()),
            search_analyzer: None,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_search_analyzer() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"search_analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: None,
            index_analyzer: None,
            search_analyzer: Some("foo".to_string()),
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_analyzer_on_integer_field() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"integer\",
            \"analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::AnalyzersOnlyAllowedOnStringType));
    }

    #[test]
    fn test_parse_analyzer_on_non_analyzed_field() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"index\": \"not_analyzed\",
            \"analyzer\": \"foo\"
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::AnalyzersOnlyAllowedOnAnalyzedFields));
    }

    #[test]
    fn test_parse_boost_default() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            boost: 1.0f64,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_boost_float() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"boost\": 2.0
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            boost: 2.0f64,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_boost_integer() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"boost\": 2
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            boost: 2.0f64,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_parse_boost_non_indexed_field() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"index\": \"no\",
            \"boost\": 2.0
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::BoostOnlyAllowedOnIndexedFields));
    }

    #[test]
    fn test_parse_boost_negative() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"boost\": -2.0
        }
        ").unwrap());

        assert_eq!(mapping, Err(FieldMappingParseError::BoostMustBePositive));
    }

    #[test]
    fn test_include_in_all_default() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_in_all: true,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_include_in_all_no() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"include_in_all\": \"no\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_in_all: false,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_include_in_all_false() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"string\",
            \"include_in_all\": false
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::String,
            is_in_all: false,
            ..FieldMappingBuilder::default()
        }));
    }

    #[test]
    fn test_include_in_all_non_string_type() {
        let mapping = parse_field(&serde_json::from_str("
        {
            \"type\": \"integer\",
            \"include_in_all\": \"yes\"
        }
        ").unwrap());

        assert_eq!(mapping, Ok(FieldMappingBuilder {
            field_type: FieldType::Integer,
            is_analyzed: false,
            is_in_all: true,
            ..FieldMappingBuilder::default()
        }));
    }
}