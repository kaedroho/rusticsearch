use std::collections::HashMap;

use rustc_serialize::json::Json;
use abra::{Term, Token, Document};

use mapping::{Mapping, FieldMapping};


#[derive(Debug)]
pub struct DocumentSource {
    pub key: String,
    pub data: Json,
}


impl DocumentSource {
    pub fn prepare(&self, mapping: &Mapping) -> Document {
        let mut fields = HashMap::new();
        let mut all_field_strings: Vec<String> = Vec::new();

        for (field_name, field_value) in self.data.as_object().unwrap() {
            if *field_value == Json::Null {
                // Treat null like a missing field
                continue;
            }

            match mapping.fields.get(field_name) {
                Some(field_mapping) => {
                    let value = field_mapping.process_value_for_index(field_value.clone());

                    match value {
                        Some(value) => {
                            // Copy the field's value into the _all field
                            if field_mapping.is_in_all {
                                if let Json::String(ref string) = *field_value {
                                    all_field_strings.push(string.clone());
                                }
                            }

                            // Insert the field
                            fields.insert(field_mapping.index_ref.expect("Attempted to prepare a document with an unlinked mapping"), value);
                        }
                        None => {
                            // TODO: Should probably be an error
                            warn!("Unprocessable value: {}", field_value);
                        }
                    }
                }
                None => {
                    // No mapping found, just insert the value as-is
                    // TODO: This should probably be an error
                    // if let Some(term) = Term::from_json(field_value) {
                    //     fields.insert(field_name.clone(), vec![Token{term: term, position: 1}]);
                    // }
                }
            }
        }

        // Insert _all field
        match mapping.fields.get("_all") {
            Some(field_mapping) => {
                let strings_json = Json::String(all_field_strings.join(" "));
                let value = field_mapping.process_value_for_index(strings_json.clone());

                match value {
                    Some(value) => {
                        fields.insert(field_mapping.index_ref.expect("Attempted to prepare a document with an unlinked mapping"), value);
                    }
                    None => {
                        // TODO: Should probably be an error
                        warn!("Unprocessable value: {}", strings_json);
                    }
                }
            }
            None => {
                // _all field disabled for this mapping
            }
        }

        Document {
            key: self.key.clone(),
            fields: fields,
        }
    }
}
