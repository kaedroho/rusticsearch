use rustc_serialize::json::Json;
use chrono::{DateTime, UTC, Timelike};
use byteorder::{WriteBytesExt, BigEndian};


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub enum Term {
    String(String),
    Boolean(bool),
    I64(i64),
    U64(u64),
    DateTime(DateTime<UTC>),
    //F64(f64),
    Null,
}

impl Term {
    pub fn from_json(json: &Json) -> Term {
        // TODO: Should be aware of mappings
        match *json {
            Json::String(ref string) => Term::String(string.clone()),
            Json::Boolean(value) => Term::Boolean(value),
            Json::F64(value) => Term::Null, //Term::F64(value),
            Json::I64(value) => Term::I64(value),
            Json::U64(value) => Term::U64(value),
            Json::Null => Term::Null,

            // These two are unsupported
            // TODO: Raise error
            Json::Array(_) => Term::Null,
            Json::Object(_) => Term::Null,
        }
    }

    pub fn as_json(&self) -> Json {
        match *self {
            Term::String(ref string) => Json::String(string.clone()),
            Term::Boolean(value) => Json::Boolean(value),
            //Term::F64(value) => Json::F64(value),
            Term::I64(value) => Json::I64(value),
            Term::U64(value) => Json::U64(value),
            Term::DateTime(value) => Json::String(value.to_rfc3339()),
            Term::Null => Json::Null,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match *self {
            Term::String(ref string) => {
                let mut bytes = Vec::with_capacity(1 + string.len());

                // Append a character to the beginning so blank strings have a value
                // Note, it's better to append to the beginning rather than the end as
                // prefixes get compressed out in trie structures
                bytes.push(b's');

                for byte in string.as_bytes() {
                    bytes.push(*byte);
                }

                bytes
            },
            Term::Boolean(value) => {
                if value {
                    vec![b't']
                } else {
                    vec![b'f']
                }
            }
            Term::I64(value) => {
                let mut bytes = Vec::with_capacity(8);
                bytes.write_i64::<BigEndian>(value);
                bytes
            }
            Term::U64(value) => {
                let mut bytes = Vec::with_capacity(8);
                bytes.write_u64::<BigEndian>(value);
                bytes
            }
            Term::DateTime(value) => {
                let mut bytes = Vec::with_capacity(0);
                let timestamp = value.timestamp();
                let micros = value.nanosecond() / 1000;
                let timestamp_with_micros = timestamp * 1000000 + micros as i64;
                bytes.write_i64::<BigEndian>(timestamp_with_micros);
                bytes
            }
            Term::Null => {
                let mut bytes = Vec::with_capacity(1);
                bytes.push(b'\0');
                bytes
            },
        }
    }
}


#[cfg(test)]
mod tests {
    use chrono::{DateTime, UTC, Timelike};
    use super::Term;

    #[test]
    fn test_string_to_bytes() {
        let term = Term::String("foo".to_string());

        assert_eq!(term.to_bytes(), vec![115, 102, 111, 111])
    }

    #[test]
    fn test_hiragana_string_to_bytes() {
        let term = Term::String("こんにちは".to_string());

        assert_eq!(term.to_bytes(), vec![115, 227, 129, 147, 227, 130, 147, 227, 129, 171, 227, 129, 161, 227, 129, 175])
    }

    #[test]
    fn test_blank_string_to_bytes() {
        let term = Term::String("".to_string());

        assert_eq!(term.to_bytes(), vec![115])
    }

    #[test]
    fn test_boolean_true_to_bytes() {
        let term = Term::Boolean(true);

        // 116 = 't' in ASCII
        assert_eq!(term.to_bytes(), vec![116])
    }

    #[test]
    fn test_boolean_false_to_bytes() {
        let term = Term::Boolean(false);

        // 102 = 'f' in ASCII
        assert_eq!(term.to_bytes(), vec![102])
    }

    #[test]
    fn test_i64_to_bytes() {
        let term = Term::I64(123);

        assert_eq!(term.to_bytes(), vec![0, 0, 0, 0, 0, 0, 0, 123])
    }

    #[test]
    fn test_negative_i64_to_bytes() {
        let term = Term::I64(-123);

        assert_eq!(term.to_bytes(), vec![255, 255, 255, 255, 255, 255, 255, 133])
    }

    #[test]
    fn test_u64_to_bytes() {
        let term = Term::U64(123);

        assert_eq!(term.to_bytes(), vec![0, 0, 0, 0, 0, 0, 0, 123])
    }

    #[test]
    fn test_datetime_to_bytes() {
        let date = "2016-07-23T16:15:00+01:00".parse::<DateTime<UTC>>().unwrap();
        let term = Term::DateTime(date);

        assert_eq!(term.to_bytes(), vec![0, 5, 56, 79, 3, 191, 101, 0])
    }

    #[test]
    fn test_datetime_with_microseconds_to_bytes() {
        let mut date = "2016-07-23T16:15:00+01:00".parse::<DateTime<UTC>>().unwrap();
        date = date.with_nanosecond(123123123).unwrap();
        let term = Term::DateTime(date);

        // This is exactly 123123 higher than the result of "test_datetime_to_bytes"
        assert_eq!(term.to_bytes(), vec![0, 5, 56, 79, 3, 193, 69, 243])
    }

    #[test]
    fn test_datetime_with_different_timezone_to_bytes() {
        let date = "2016-07-23T16:15:00+02:00".parse::<DateTime<UTC>>().unwrap();
        let term = Term::DateTime(date);

        // This is exactly 3_600_000_000 lower than the result of "test_datetime_to_bytes"
        assert_eq!(term.to_bytes(), vec![0, 5, 56, 78, 45, 43, 193, 0])
    }

    #[test]
    fn test_null_to_bytes() {
        let term = Term::Null;

        assert_eq!(term.to_bytes(), vec![0])
    }
}
