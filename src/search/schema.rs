use std::collections::HashMap;
use std::ops::Deref;
use std::fmt;

use serde::{Serialize, Deserialize, Serializer, Deserializer};
use fnv::FnvHashMap;

bitflags! {
    pub flags FieldFlags: u32 {
        const FIELD_INDEXED = 0b00000001,
        const FIELD_STORED  = 0b00000010,
    }
}

impl Serialize for FieldFlags {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let mut flag_strings = Vec::new();

        if self.contains(FIELD_INDEXED) {
            flag_strings.push("INDEXED");
        }

        if self.contains(FIELD_STORED) {
            flag_strings.push("STORED");
        }

        serializer.serialize_str(&flag_strings.join("|"))
    }
}

impl<'a> Deserialize<'a> for FieldFlags {
    fn deserialize<D>(deserializer: D) -> Result<FieldFlags, D::Error>
        where D: Deserializer<'a>
    {
        struct Visitor;

        impl<'a> ::serde::de::Visitor<'a> for Visitor {
            type Value = FieldFlags;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string of flag names separated by a '|' character")
            }

            fn visit_str<E>(self, value: &str) -> Result<FieldFlags, E>
                where E: ::serde::de::Error
            {
                let mut flags = FieldFlags::empty();

                for flag_s in value.split("|") {
                    match flag_s {
                        "INDEXED" => {
                            flags |= FIELD_INDEXED;
                        }
                        "STORED" => {
                            flags |= FIELD_STORED;
                        }
                        _ => {} // TODO: error
                    }
                }

                Ok(flags)
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    Text,
    PlainString,
    I64,
    Boolean,
    DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldInfo {
    name: String,
    pub field_type: FieldType,
    pub field_flags: FieldFlags,
}

impl FieldInfo {
    pub fn new(name: String, field_type: FieldType, field_flags: FieldFlags) -> FieldInfo {
        FieldInfo {
            name: name,
            field_type: field_type,
            field_flags: field_flags,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct FieldId(pub u32);

// FieldId needs to be serialised as a string as it's used as a mapping key
impl Serialize for FieldId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'a> Deserialize<'a> for FieldId {
    fn deserialize<D>(deserializer: D) -> Result<FieldId, D::Error>
        where D: Deserializer<'a>
    {
        struct Visitor;

        impl<'a> ::serde::de::Visitor<'a> for Visitor {
            type Value = FieldId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string containing an integer")
            }

            fn visit_str<E>(self, value: &str) -> Result<FieldId, E>
                where E: ::serde::de::Error
            {
                match value.parse() {
                    Ok(value) => Ok(FieldId(value)),
                    Err(_) => Err(E::invalid_value(::serde::de::Unexpected::Str(value), &"a string containing an integer")),
                }
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

#[derive(Debug)]
pub enum AddFieldError {
    FieldAlreadyExists(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    next_field_id: u32,
    fields: FnvHashMap<FieldId, FieldInfo>,
    field_names: HashMap<String, FieldId>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema {
            next_field_id: 1,
            fields: FnvHashMap::default(),
            field_names: HashMap::new(),
        }
    }

    fn new_field_id(&mut self) -> FieldId {
        let field_id = FieldId(self.next_field_id);
        self.next_field_id += 1;

        field_id
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<FieldId> {
        self.field_names.get(name).cloned()
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType, field_flags: FieldFlags) -> Result<FieldId, AddFieldError> {
        if self.field_names.contains_key(&name) {
            return Err(AddFieldError::FieldAlreadyExists(name));
        }

        let field_id = self.new_field_id();
        let field_info = FieldInfo::new(name.clone(), field_type, field_flags);

        self.fields.insert(field_id, field_info);
        self.field_names.insert(name, field_id);

        Ok(field_id)
    }

    pub fn remove_field(&mut self, field_id: &FieldId) -> bool {
        match self.fields.remove(field_id) {
            Some(removed_field) => {
                self.field_names.remove(&removed_field.name);
                true
            }
            None => false
        }
    }
}

impl Deref for Schema {
    type Target = FnvHashMap<FieldId, FieldInfo>;

    fn deref(&self) -> &FnvHashMap<FieldId, FieldInfo> {
        &self.fields
    }
}
