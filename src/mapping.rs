use rustc_serialize::json::Json;


#[derive(Debug)]
pub enum FieldType {
    String,
    Binary,
    Number{size: u8, is_float: bool},
    Boolean,
    Date,
}


impl Default for FieldType {
    fn default() -> FieldType { FieldType::String }
}



#[derive(Debug)]
pub struct FieldMapping {
    data_type: FieldType,
    is_indexed: bool,
    is_stored: bool,
    is_in_all: bool,
    boost: f64,
}


impl Default for FieldMapping {
    fn default() -> FieldMapping {
        FieldMapping {
            data_type: FieldType::default(),
            is_indexed: true,
            is_stored: false,
            is_in_all: true,
            boost: 1.0f64,
        }
    }
}


fn parse_boolean(json: &Json) -> bool {
    match *json {
        Json::Boolean(val) => val,
        Json::String(ref s) => {
            match s.as_ref() {
                "yes" => true,
                "no" => false,
                _ => {
                    warn!("bad boolean value {:?}", s);
                    false
                }
            }
        }
        _ => {
            // TODO: Raise error
            warn!("bad boolean value {:?}", json);
            false
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
                        "string" => FieldType::String,
                        "integer" => FieldType::Number{size: 64, is_float: false},
                        "boolean" => FieldType::Boolean,
                        "date" => FieldType::Date,
                        _ => {
                            // TODO; make this an error
                            warn!("unimplemented type name! {}", type_name);
                            FieldType::default()
                        }
                    };
                }
                "index" => {
                    let index = value.as_string().unwrap();
                    if index == "not_analyzed" {
                        field_mapping.is_indexed = false;
                    } else {
                        // TODO: Implement other variants and make this an error
                        warn!("unimplemented index setting! {}", index);
                    }
                }
                "index_analyzer" => {
                    // TODO
                }
                "boost" => {
                    field_mapping.boost = value.as_f64().unwrap();
                }
                "store" => {
                    field_mapping.is_stored = parse_boolean(value);
                }
                "include_in_all" => {
                    field_mapping.is_in_all = parse_boolean(value);
                }
                _ => warn!("unimplemented field mapping key! {}", key)
            }

        }

        field_mapping
    }
}
