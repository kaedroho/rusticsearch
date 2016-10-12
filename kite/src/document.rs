use std::collections::HashMap;

use chrono::{DateTime, UTC, Timelike};
use byteorder::{WriteBytesExt, BigEndian};

use token::Token;


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
}


#[derive(Debug)]
pub struct Document {
    pub key: String,
    pub indexed_fields: HashMap<String, Vec<Token>>,
    pub stored_fields: HashMap<String, FieldValue>,
}
