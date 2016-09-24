use std::collections::HashMap;

use serde_json;
use kite::Document;

use mapping::{Mapping, MappingProperty};


#[derive(Debug)]
pub struct DocumentSource {
    pub key: String,
    pub data: serde_json::Value,
}


impl DocumentSource {
    pub fn prepare(&self, mapping: &Mapping) -> Document {
        let mut indexed_fields = HashMap::new();
        let mut stored_fields = HashMap::new();
        let mut all_field_strings: Vec<String> = Vec::new();

        for (field_name, field_value) in self.data.as_object().unwrap() {
            if *field_value == serde_json::Value::Null {
                // Treat null like a missing field
                continue;
            }

            match mapping.properties.get(field_name) {
                Some(&MappingProperty::Field(ref field_mapping)) => {
                    if field_mapping.is_indexed {
                        let value = field_mapping.process_value_for_index(field_value.clone());

                        match value {
                            Some(value) => {
                                // Copy the field's value into the _all field
                                if field_mapping.is_in_all {
                                    if let serde_json::Value::String(ref string) = *field_value {
                                        all_field_strings.push(string.clone());
                                    }
                                }

                                // Insert the field
                                indexed_fields.insert(field_mapping.index_ref.unwrap(), value);
                            }
                            None => {
                                // TODO: Should probably be an error
                                warn!("Unprocessable value: {}", field_value);
                            }
                        }
                    }

                    if field_mapping.is_stored {
                        let value = field_mapping.process_value_for_store(field_value.clone());

                        match value {
                            Some(value) => {
                                // Insert the field
                                stored_fields.insert(field_mapping.index_ref.unwrap(), value);
                            }
                            None => {
                                // TODO: Should probably be an error
                                warn!("Unprocessable value: {}", field_value);
                            }
                        }
                    }
                }
                None => {
                    // No mapping found
                    // TODO: This should probably be an error
                }
            }
        }

        // Insert _all field
        match mapping.properties.get("_all") {
            Some(property) => {
                match *property {
                    MappingProperty::Field(ref field_mapping) => {
                        let strings_json = serde_json::Value::String(all_field_strings.join(" "));
                        let value = field_mapping.process_value_for_index(strings_json.clone());

                        match value {
                            Some(value) => {
                                indexed_fields.insert(field_mapping.index_ref.unwrap(), value);
                            }
                            None => {
                                // TODO: Should probably be an error
                                warn!("Unprocessable value: {}", strings_json);
                            }
                        }
                    }
                }
            }
            None => {
                // _all field disabled for this mapping
            }
        }

        Document {
            key: self.key.clone(),
            indexed_fields: indexed_fields,
            stored_fields: stored_fields,
        }
    }
}
