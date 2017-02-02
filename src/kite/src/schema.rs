use std::collections::HashMap;
use std::ops::Deref;

use rustc_serialize::{Encodable, Decodable, Encoder, Decoder};


bitflags! {
    pub flags FieldFlags: u32 {
        const FIELD_INDEXED = 0b00000001,
        const FIELD_STORED  = 0b00000010,
    }
}


impl Encodable for FieldFlags {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let mut flag_strings = Vec::new();

        if self.contains(FIELD_INDEXED) {
            flag_strings.push("INDEXED");
        }

        if self.contains(FIELD_STORED) {
            flag_strings.push("STORED");
        }

        try!(s.emit_str(&flag_strings.join("|")));

        Ok(())
    }
}

impl Decodable for FieldFlags {
    fn decode<D: Decoder>(d: &mut D) -> Result<FieldFlags, D::Error> {
        let s = try!(d.read_str());
        let mut flags = FieldFlags::empty();

        for flag_s in s.split("|") {
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


#[derive(Debug, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum FieldType {
    Text,
    PlainString,
    I64,
    Boolean,
    DateTime,
}


#[derive(Debug, Clone, RustcEncodable, RustcDecodable)]
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
pub struct FieldRef(u32);


impl FieldRef {
    pub fn ord(&self) -> u32 {
        self.0
    }
}


impl Encodable for FieldRef {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        s.emit_u32(self.ord())
    }
}

impl Decodable for FieldRef {
    fn decode<D: Decoder>(d: &mut D) -> Result<FieldRef, D::Error> {
        Ok(FieldRef(try!(d.read_u32())))
    }
}


#[derive(Debug)]
pub enum AddFieldError {
    FieldAlreadyExists(String),
}


#[derive(Debug, Clone, RustcEncodable, RustcDecodable)]
pub struct Schema {
    next_field_id: u32,
    fields: HashMap<FieldRef, FieldInfo>,
    field_names: HashMap<String, FieldRef>,
}


impl Schema {
    pub fn new() -> Schema {
        Schema {
            next_field_id: 1,
            fields: HashMap::new(),
            field_names: HashMap::new(),
        }
    }

    fn new_field_ref(&mut self) -> FieldRef {
        let field_ref = FieldRef(self.next_field_id);
        self.next_field_id += 1;

        field_ref
    }

    pub fn get_field_by_name(&self, name: &str) -> Option<FieldRef> {
        self.field_names.get(name).cloned()
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType, field_flags: FieldFlags) -> Result<FieldRef, AddFieldError> {
        if self.field_names.contains_key(&name) {
            return Err(AddFieldError::FieldAlreadyExists(name));
        }

        let field_ref = self.new_field_ref();
        let field_info = FieldInfo::new(name.clone(), field_type, field_flags);

        self.fields.insert(field_ref, field_info);
        self.field_names.insert(name, field_ref);

        Ok(field_ref)
    }

    pub fn remove_field(&mut self, field_ref: &FieldRef) -> bool {
        match self.fields.remove(field_ref) {
            Some(removed_field) => {
                self.field_names.remove(&removed_field.name);
                true
            }
            None => false
        }
    }
}


impl Deref for Schema {
    type Target = HashMap<FieldRef, FieldInfo>;

    fn deref(&self) -> &HashMap<FieldRef, FieldInfo> {
        &self.fields
    }
}
