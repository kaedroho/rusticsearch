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
            let processed_value = if let Some(field_mapping) = mapping.fields.get(field_name) {
                let value = field_mapping.process_value_for_index(field_value.clone());

                match value {
                    Some(ref value) => {
                        if field_mapping.is_in_all {
                            all_field_tokens.extend(value.iter().cloned());
                        }
                    }
                    None => {
                        warn!("Unprocessable value: {}", field_value);
                    }
                }

                value
            } else {
                Some(vec![Term::from_json(field_value)])
            };

            if let Some(field_value) = processed_value {
                fields.insert(field_name.clone(), field_value);
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
