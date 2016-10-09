use std::collections::HashMap;

use chrono::{DateTime, UTC, Timelike};
use byteorder::{WriteBytesExt, BigEndian};

use token::Token;


#[derive(Debug)]
pub enum StoredFieldValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    DateTime(DateTime<UTC>),
}


impl StoredFieldValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match *self {
            StoredFieldValue::String(ref string) => {
                let mut bytes = Vec::with_capacity(string.len());

                for byte in string.as_bytes() {
                    bytes.push(*byte);
                }

                bytes
            }
            StoredFieldValue::Integer(value) => {
                let mut bytes = Vec::with_capacity(8);
                bytes.write_i64::<BigEndian>(value).unwrap();
                bytes
            }
            StoredFieldValue::Boolean(value) => {
                if value {
                    vec![b't']
                } else {
                    vec![b'f']
                }
            }
            StoredFieldValue::DateTime(value) => {
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
    pub fields: HashMap<String, Vec<Token>>,
    pub stored_fields: HashMap<String, StoredFieldValue>,
}
