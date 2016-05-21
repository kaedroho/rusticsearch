use std::collections::BTreeMap;

use rustc_serialize::json::Json;

use term::Term;
use mapping::Mapping;


#[derive(Debug)]
pub struct Document {
    pub id: String,
    pub fields: BTreeMap<String, Vec<Term>>,
}

impl Document {
    pub fn from_json(id: String, data: Json, mapping: &Mapping) -> Document {
        let mut fields = BTreeMap::new();
        let mut all_field_tokens: Vec<Term> = Vec::new();

        for (field_name, field_value) in data.as_object().unwrap() {
            match mapping.fields.get(field_name) {
                Some(field_mapping) => {
                    let value = field_mapping.process_value_for_index(field_value.clone());

                    match value {
                        Some(value) => {
                            // Copy the field's value into the _all field
                            if field_mapping.is_in_all {
                                all_field_tokens.extend(value.iter().cloned());
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
                    fields.insert(field_name.clone(), vec![Term::from_json(field_value)]);
                }
            }
        }

        // Insert _all field
        fields.insert("_all".to_owned(), all_field_tokens);

        Document {
            id: id,
            fields: fields,
        }
    }
}
