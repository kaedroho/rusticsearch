use std::collections::BTreeMap;

use rustc_serialize::json::Json;

use search::term::Term;
use search::token::Token;
use search::mapping::Mapping;


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
        let mut all_field_tokens: Vec<Token> = Vec::new();

        for (field_name, field_value) in self.data.as_object().unwrap() {
            match mapping.fields.get(field_name) {
                Some(field_mapping) => {
                    let value = field_mapping.process_value_for_index(field_value.clone());

                    match value {
                        Some(value) => {
                            // Copy the field's value into the _all field
                            if field_mapping.is_in_all {
                                // TODO: Should the positions be updated?
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
                    if let Some(term) = Term::from_json(field_value) {
                        fields.insert(field_name.clone(), vec![Token{term: term, position: 1}]);
                    }
                }
            }
        }

        // Insert _all field
        fields.insert("_all".to_owned(), all_field_tokens);

        Document {
            key: self.key.clone(),
            fields: fields,
        }
    }
}
