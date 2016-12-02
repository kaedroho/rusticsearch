use std::collections::HashMap;

use rustc_serialize::json::Json;
use chrono::{DateTime, UTC, Timelike};
use byteorder::{WriteBytesExt, BigEndian};

use token::Token;


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


#[derive(Debug)]
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

    pub fn as_json(&self) -> Json {
        match *self {
            FieldValue::String(ref string) => Json::String(string.clone()),
            FieldValue::Boolean(value) => Json::Boolean(value),
            FieldValue::Integer(value) => Json::I64(value),
            FieldValue::DateTime(value) => Json::String(value.to_rfc3339()),
        }
    }
}


#[derive(Debug)]
pub struct Document {
    pub key: String,
    pub indexed_fields: HashMap<String, Vec<Token>>,
    pub stored_fields: HashMap<String, FieldValue>,
}
