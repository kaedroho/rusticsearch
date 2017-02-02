use std::collections::HashMap;

use serde;
use chrono::{DateTime, UTC, Timelike};
use byteorder::{WriteBytesExt, BigEndian};

use token::Token;
use schema::FieldRef;


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct DocRef(u32, u16);


impl DocRef {
    pub fn segment(&self) -> u32 {
        self.0
    }

    pub fn ord(&self) -> u16 {
        self.1
    }

    pub fn as_u64(&self) -> u64 {
        (self.0 as u64) << 16 | (self.1 as u64)
    }

    pub fn from_segment_ord(segment: u32, ord: u16) -> DocRef {
        DocRef(segment, ord)
    }

    pub fn from_u64(val: u64) -> DocRef {
        let segment = (val >> 16) & 0xFFFFFFFF;
        let ord = val & 0xFFFF;
        DocRef(segment as u32, ord as u16)
    }
}


#[derive(Debug, Clone)]
pub enum FieldValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    DateTime(DateTime<UTC>),
}


impl FieldValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match *self {
            FieldValue::String(ref string) => {
                let mut bytes = Vec::with_capacity(string.len());

                for byte in string.as_bytes() {
                    bytes.push(*byte);
                }

                bytes
            }
            FieldValue::Integer(value) => {
                let mut bytes = Vec::with_capacity(8);
                bytes.write_i64::<BigEndian>(value).unwrap();
                bytes
            }
            FieldValue::Boolean(value) => {
                if value {
                    vec![b't']
                } else {
                    vec![b'f']
                }
            }
            FieldValue::DateTime(value) => {
                let mut bytes = Vec::with_capacity(0);
                let timestamp = value.timestamp();
                let micros = value.nanosecond() / 1000;
                let timestamp_with_micros = timestamp * 1000000 + micros as i64;
                bytes.write_i64::<BigEndian>(timestamp_with_micros).unwrap();
                bytes
            }
        }
    }
}


impl serde::Serialize for FieldValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer
    {
        match *self {
            FieldValue::String(ref string) => serializer.serialize_str(string),
            FieldValue::Boolean(value) => serializer.serialize_bool(value),
            FieldValue::Integer(value) => serializer.serialize_i64(value),
            FieldValue::DateTime(value) => serializer.serialize_str(&value.to_rfc3339()),
        }
    }
}


#[derive(Debug, Clone)]
pub struct Document {
    pub key: String,
    pub indexed_fields: HashMap<FieldRef, Vec<Token>>,
    pub stored_fields: HashMap<FieldRef, FieldValue>,
}
