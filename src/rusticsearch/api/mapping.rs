use std::collections::HashMap;
use std::sync::Arc;

use kite::schema::{FieldType, FieldFlags, FIELD_INDEXED, FIELD_STORED};
use serde_json::Value;
use rocket::State;
use rocket_contrib::JSON;

use system::System;
use mapping::{self, MappingProperty};
use mapping::parse::parse as parse_mapping;


#[put("/<index_name>/_mapping/<mapping_name>", data = "<data>")]
pub fn put(index_name: &str, mapping_name: &str, data: JSON<Value>, system: State<Arc<System>>) -> Option<JSON<Value>> {
    // Lock cluster metadata
    let mut cluster_metadata = system.metadata.write().unwrap();

    // Get index
    let mut index = get_index_or_404_mut!(cluster_metadata, index_name);

    let data = data.as_object().unwrap().get(mapping_name).unwrap();

    // Insert mapping
    let mapping_builder = match parse_mapping(&data) {
        Ok(mapping_builder) => mapping_builder,
        Err(_) => {
            // TODO: Better error
            return Some(JSON(json!({"acknowledged": false})));  // TODO 400 error
        }
    };
    let mut index_metadata = index.metadata.write().unwrap();
    let mut mapping = mapping_builder.build(&index_metadata);
    debug!("{:#?}", mapping);
    let is_updating = index_metadata.mappings.contains_key(mapping_name);

    // Find list of new fields that need to be added to the store
    let new_fields = {
        let index_reader = index.store.reader();
        let schema = index_reader.schema();
        let mut new_fields: HashMap<String, (FieldType, FieldFlags)>  = HashMap::new();
        for (name, property) in mapping.properties.iter() {
            if let MappingProperty::Field(ref field_mapping) = *property {
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
                if let Some(field_ref) = schema.get_field_by_name(&name) {
                    let field_info = schema.get(&field_ref).expect("get_field_by_name returned an invalid FieldRef");

                    // Field already exists. Check for conflicting type or flags, otherwise ignore.
                    if field_info.field_type == field_type && field_info.field_flags == field_flags {
                        continue;
                    } else {
                        // Conflict!
                        // TODO: Better error
                        return Some(JSON(json!({"acknowledged": false})));  // TODO 400 error
                    }
                }

                new_fields.insert(name.clone(), (field_type, field_flags));
            }
        }

        new_fields
    };

    // Add new fields into the store
    for (field_name, (field_type, field_flags)) in new_fields {
        let indexed_yesno = if field_flags.contains(FIELD_INDEXED) { "yes" } else { "no" };
        let stored_yesno = if field_flags.contains(FIELD_STORED) { "yes" } else { "no" };
        system.log.info("[api] adding field", b!("index" => index_name, "field" => field_name, "type" => format!("{:?}", field_type), "indexed" => indexed_yesno, "stored" => stored_yesno));

        index.store.add_field(field_name, field_type, field_flags).unwrap();
    }

    // Link the mapping
    {
        let index_reader = index.store.reader();
        let schema = index_reader.schema();

        for (name, property) in mapping.properties.iter_mut() {
            if let MappingProperty::Field(ref mut field_mapping) = *property {
                field_mapping.index_ref = schema.get_field_by_name(&name)
            }
        }
    }

    index_metadata.mappings.insert(mapping_name.clone().to_owned(), mapping);
    index_metadata.save(index.metadata_path()).unwrap();

    if is_updating {
        // TODO: New mapping should be merged with existing one
        system.log.info("[api] updated mapping", b!("index" => index_name, "mapping" => mapping_name));
    } else {
        system.log.info("[api] created mapping", b!("index" => index_name, "mapping" => mapping_name));
    }

    Some(JSON(json!({"acknowledged": true})))
}
