use std::io::Read;
use std::collections::HashMap;

use kite::schema::{FieldType, FieldFlags, FIELD_INDEXED, FIELD_STORED};
use rustc_serialize::json::Json;

use mapping;
use mapping::parse::parse as parse_mapping;

use api::persistent;
use api::iron::prelude::*;
use api::iron::status;
use api::router::Router;
use api::utils::json_response;


pub fn view_put_mapping(req: &mut Request) -> IronResult<Response> {
    let ref system = get_system!(req);
    let ref index_name = read_path_parameter!(req, "index").unwrap_or("");
    let ref mapping_name = read_path_parameter!(req, "mapping").unwrap_or("");

    // Lock index array
    let mut indices = system.indices.write().unwrap();

    // Get index
    let mut index = get_index_or_404_mut!(indices, *index_name);

    // Load data from body
    let data = json_from_request_body!(req);

    let data = match data {
        Some(data) => data,
        None => {
            // TODO: Better error
            return Ok(json_response(status::BadRequest, "{\"acknowledged\": false}"));
        }
    };

    let data = data.as_object().unwrap().get(*mapping_name).unwrap();

    // Insert mapping
    let mapping_builder = match parse_mapping(&data) {
        Ok(mapping_builder) => mapping_builder,
        Err(_) => {
            // TODO: Better error
            return Ok(json_response(status::BadRequest, "{\"acknowledged\": false}"));
        }
    };
    let mut mapping = mapping_builder.build(&index.settings.analyzers);
    debug!("{:#?}", mapping);
    let is_updating = index.mappings.contains_key(*mapping_name);

    // Find list of new fields that need to be added to the store
    let new_fields = {
        let index_reader = index.store.reader();
        let schema = index_reader.schema();
        let mut new_fields: HashMap<String, (FieldType, FieldFlags)>  = HashMap::new();
        for (field_name, field_mapping) in mapping.fields.iter() {
            let field_type = match field_mapping.data_type {
                mapping::FieldType::String => FieldType::Text,
                mapping::FieldType::Integer => FieldType::I64,
                mapping::FieldType::Boolean => FieldType::Boolean,
                mapping::FieldType::Date => FieldType::DateTime,
            };

            // Flags
            let mut field_flags = FieldFlags::empty();

            if field_mapping.is_indexed {
                field_flags |= FIELD_INDEXED;
            }

            if field_mapping.is_stored {
                field_flags |= FIELD_STORED;
            }

            // Check if this field already exists
            if let Some(field_ref) = schema.get_field_by_name(&field_name) {
                let field_info = schema.get(&field_ref).expect("get_field_by_name returned an invalid FieldRef");

                // Field already exists. Check for conflicting type or flags, otherwise ignore.
                if field_info.field_type == field_type && field_info.field_flags == field_flags {
                    continue;
                } else {
                    // Conflict!
                    // TODO: Better error
                    return Ok(json_response(status::BadRequest, "{\"acknowledged\": false}"));
                }
            }

            new_fields.insert(field_name.clone(), (field_type, field_flags));
        }

        new_fields
    };

    // Add new fields into the store
    for (field_name, (field_type, field_flags)) in new_fields {
        let indexed_yesno = if field_flags.contains(FIELD_INDEXED) { "yes" } else { "no" };
        let stored_yesno = if field_flags.contains(FIELD_STORED) { "yes" } else { "no" };
        system.log.info("[api] adding field", b!("index" => *index_name, "field" => field_name, "type" => format!("{:?}", field_type), "indexed" => indexed_yesno, "stored" => stored_yesno));

        index.store.add_field(field_name, field_type, field_flags).unwrap();
    }

    // Link the mapping
    {
        let index_reader = index.store.reader();
        let schema = index_reader.schema();

        for (field_name, field_mapping) in mapping.fields.iter_mut() {
            field_mapping.index_ref = schema.get_field_by_name(&field_name)
        }
    }

    index.mappings.insert(mapping_name.clone().to_owned(), mapping);

    if is_updating {
        // TODO: New mapping should be merged with existing one
        system.log.info("[api] updated mapping", b!("index" => *index_name, "mapping" => *mapping_name));
    } else {
        system.log.info("[api] created mapping", b!("index" => *index_name, "mapping" => *mapping_name));
    }

    return Ok(json_response(status::Ok, "{\"acknowledged\": true}"));
}
