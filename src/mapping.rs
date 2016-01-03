use rustc_serialize::json::Json;

use super::types::ESType;


#[derive(Debug)]
pub struct FieldMapping {
    data_type: ESType,
    is_indexed: bool,
    is_stored: bool,
    is_in_all: bool,
    boost: f64,
}


impl Default for FieldMapping {
    fn default() -> FieldMapping {
        FieldMapping {
            data_type: ESType::default(),
            is_indexed: true,
            is_stored: false,
            is_in_all: true,
            boost: 1.0f64,
        }
    }
}


impl FieldMapping {
    pub fn from_json(json: &Json) -> FieldMapping {
        let json = json.as_object().unwrap();
        let mut field_mapping = FieldMapping::default();

        for (key, value) in json.iter() {
            match key.as_ref() {
                "type" => {
                    let type_name = value.as_string().unwrap();

                    field_mapping.data_type = match type_name.as_ref() {
                        "string" => ESType::String,
                        "integer" => ESType::Number{bits: 64, is_float: false},
                        "boolean" => ESType::Boolean,
                        "date" => ESType::Date,
                        _ => {
                            // TODO; make this an error
                            println!("unimplemented type name! {}", type_name);
                            ESType::default()
                        }
                    };
                }
                "index" => {

                }
                "index_analyzer" => {

                }
                "boost" => {

                }
                "store" => {

                }
                "include_in_all" => {

                }
                _ => println!("unimplemented field mapping key! {}", key)
            }

        }

        field_mapping
    }
}
