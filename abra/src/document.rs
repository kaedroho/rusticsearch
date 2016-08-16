use std::collections::BTreeMap;

use rustc_serialize::json::Json;

use term::Term;
use token::Token;
use mapping::{Mapping, FieldMapping, FieldType};


#[derive(Debug)]
pub struct Document {
    pub key: String,
    pub fields: BTreeMap<String, Vec<Token>>,
}


#[derive(Debug)]
pub struct DocumentSource {
    pub key: String,
    pub data: Json,
}


impl DocumentSource {
    pub fn prepare(&self, mapping: &Mapping) -> Document {
        let mut fields = BTreeMap::new();
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
                            fields.insert(field_name.clone(), value);
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
                    if let Some(term) = Term::from_json(field_value) {
                        fields.insert(field_name.clone(), vec![Token{term: term, position: 1}]);
                    }
                }
            }
        }

        // Insert _all field
        let all_field_mapping = FieldMapping::default();
        let all_field_strings_json = Json::String(all_field_strings.join(" "));
        let all_field_value = all_field_mapping.process_value_for_index(all_field_strings_json.clone());

        match all_field_value {
            Some(all_field_value) => {
                fields.insert("_all".to_owned(), all_field_value);
            }
            None => {
                // TODO: Should probably be an error
                warn!("Unprocessable value: {}", all_field_strings_json);
            }
        }

        Document {
            key: self.key.clone(),
            fields: fields,
        }
    }
}
