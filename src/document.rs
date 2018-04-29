use serde_json;
use search::Document;
use fnv::FnvHashMap;

use mapping::{Mapping, MappingProperty, FieldValueError};


#[derive(Debug)]
pub struct DocumentSource<'a> {
    pub key: &'a str,
    pub data: &'a serde_json::Map<String, serde_json::Value>,
}


#[derive(Debug)]
pub enum PrepareDocumentError {
    FieldDoesntExist {
        field_name: String,
    },
    FieldValueError {
        field_name: String,
        value: serde_json::Value,
        error: FieldValueError,
    },
}


impl<'a> DocumentSource<'a> {
    pub fn prepare(&self, mapping: &Mapping) -> Result<Document, PrepareDocumentError> {
        let mut indexed_fields = FnvHashMap::default();
        let mut stored_fields = FnvHashMap::default();
        let mut all_field_strings: Vec<String> = Vec::new();

        for (field_name, field_value) in self.data {
            if *field_value == serde_json::Value::Null {
                // Treat null like a missing field
                continue;
            }

            match mapping.properties.get(field_name) {
                Some(&MappingProperty::Field(ref field_mapping)) => {
                    if field_mapping.is_indexed {
                        let value = field_mapping.process_value_for_index(field_value);

                        match value {
                            Ok(Some(value)) => {
                                // Copy the field's value into the _all field
                                if field_mapping.is_in_all {
                                    if let serde_json::Value::String(ref string) = *field_value {
                                        all_field_strings.push(string.clone());
                                    }
                                }

                                // Insert the field
                                indexed_fields.insert(field_mapping.index_ref.unwrap(), value);
                            }
                            Ok(None) => {}
                            Err(error) => {
                                return Err(PrepareDocumentError::FieldValueError {
                                    field_name: field_name.clone(),
                                    value: field_value.clone(),
                                    error: error,
                                });
                            }
                        }
                    }

                    if field_mapping.is_stored {
                        let value = field_mapping.process_value_for_store(field_value);

                        match value {
                            Ok(Some(value)) => {
                                // Insert the field
                                stored_fields.insert(field_mapping.index_ref.unwrap(), value);
                            }
                            Ok(None) => {}
                            Err(error) => {
                                return Err(PrepareDocumentError::FieldValueError {
                                    field_name: field_name.clone(),
                                    value: field_value.clone(),
                                    error: error,
                                });
                            }
                        }
                    }
                }
                Some(&MappingProperty::NestedMapping(ref _nested_mapping)) => {
                    // TODO
                }
                None => {
                    // No mapping found
                    return Err(PrepareDocumentError::FieldDoesntExist {
                        field_name: field_name.clone(),
                    });
                }
            }
        }

        // Insert _all field
        if let Some(property) = mapping.properties.get("_all") {
            if let MappingProperty::Field(ref field_mapping) = *property {
                let strings_json = serde_json::Value::String(all_field_strings.join(" "));
                let value = field_mapping.process_value_for_index(&strings_json);

                match value {
                    Ok(Some(value)) => {
                        indexed_fields.insert(field_mapping.index_ref.unwrap(), value);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        return Err(PrepareDocumentError::FieldValueError {
                            field_name: "_all".to_string(),
                            value: strings_json.clone(),
                            error: error,
                        });
                    }
                }
            }
        }

        Ok(Document {
            key: self.key.to_string(),
            indexed_fields: indexed_fields,
            stored_fields: stored_fields,
        })
    }
}
