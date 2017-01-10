use rustc_serialize::json::Json;
use chrono::{DateTime, UTC, Timelike};
use byteorder::{WriteBytesExt, BigEndian};


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct TermRef(u32);


impl TermRef {
    pub fn new(ord: u32) -> TermRef {
        TermRef(ord)
    }

    pub fn ord(&self) -> u32 {
        self.0
    }
}


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct Term(Vec<u8>);


impl Term {
    pub fn from_json(json: &Json) -> Option<Term> {
        // TODO: Should be aware of mappings
        match *json {
            Json::String(ref string) => Some(Term::from_string(string.clone())),
            Json::Boolean(value) => Some(Term::from_boolean(value)),
            Json::F64(_) => None,
            Json::I64(value) => Some(Term::from_integer(value)),
            Json::U64(value) => Some(Term::from_integer(value as i64)),  // FIXME
            Json::Null => None,
            Json::Array(_) => None,
            Json::Object(_) => None,
        }
    }

    pub fn from_string(string: String) -> Term {
        let mut bytes = Vec::with_capacity(string.len());

        for byte in string.as_bytes() {
            bytes.push(*byte);
        }

        Term(bytes)
    }

    pub fn from_boolean(value: bool) -> Term {
        if value {
            Term(vec![b't'])
        } else {
            Term(vec![b'f'])
        }
    }

    pub fn from_integer(value: i64) -> Term {
        let mut bytes = Vec::with_capacity(8);
        bytes.write_i64::<BigEndian>(value).unwrap();
        Term(bytes)
    }

    pub fn from_datetime(value: &DateTime<UTC>) -> Term {
        let mut bytes = Vec::with_capacity(0);
        let timestamp = value.timestamp();
        let micros = value.nanosecond() / 1000;
        let timestamp_with_micros = timestamp * 1000000 + micros as i64;
        bytes.write_i64::<BigEndian>(timestamp_with_micros).unwrap();
        Term(bytes)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }
}


#[cfg(test)]
mod tests {
    use chrono::{DateTime, UTC, Timelike};
    use super::Term;

    #[test]
    fn test_string_to_bytes() {
        let term = Term::from_string("foo".to_string());

        assert_eq!(term.to_bytes(), vec![102, 111, 111])
    }

    #[test]
    fn test_hiragana_string_to_bytes() {
        let term = Term::from_string("こんにちは".to_string());

        assert_eq!(term.to_bytes(), vec![227, 129, 147, 227, 130, 147, 227, 129, 171, 227, 129, 161, 227, 129, 175])
    }

    #[test]
    fn test_blank_string_to_bytes() {
        let term = Term::from_string("".to_string());

        assert_eq!(term.to_bytes(), vec![])
    }

    #[test]
    fn test_boolean_true_to_bytes() {
        let term = Term::from_boolean(true);

        // 116 = 't' in ASCII
        assert_eq!(term.to_bytes(), vec![116])
    }

    #[test]
    fn test_boolean_false_to_bytes() {
        let term = Term::from_boolean(false);

        // 102 = 'f' in ASCII
        assert_eq!(term.to_bytes(), vec![102])
    }

    #[test]
    fn test_integer_to_bytes() {
        let term = Term::from_integer(123);

        assert_eq!(term.to_bytes(), vec![0, 0, 0, 0, 0, 0, 0, 123])
    }

    #[test]
    fn test_negative_integer_to_bytes() {
        let term = Term::from_integer(-123);

        assert_eq!(term.to_bytes(), vec![255, 255, 255, 255, 255, 255, 255, 133])
    }

    #[test]
    fn test_datetime_to_bytes() {
        let date = "2016-07-23T16:15:00+01:00".parse::<DateTime<UTC>>().unwrap();
        let term = Term::from_datetime(&date);

        assert_eq!(term.to_bytes(), vec![0, 5, 56, 79, 3, 191, 101, 0])
    }

    #[test]
    fn test_datetime_with_microseconds_to_bytes() {
        let mut date = "2016-07-23T16:15:00+01:00".parse::<DateTime<UTC>>().unwrap();
        date = date.with_nanosecond(123123123).unwrap();
        let term = Term::from_datetime(&date);

        // This is exactly 123123 higher than the result of "test_datetime_to_bytes"
        assert_eq!(term.to_bytes(), vec![0, 5, 56, 79, 3, 193, 69, 243])
    }

    #[test]
    fn test_datetime_with_different_timezone_to_bytes() {
        let date = "2016-07-23T16:15:00+02:00".parse::<DateTime<UTC>>().unwrap();
        let term = Term::from_datetime(&date);

        // This is exactly 3_600_000_000 lower than the result of "test_datetime_to_bytes"
        assert_eq!(term.to_bytes(), vec![0, 5, 56, 78, 45, 43, 193, 0])
    }
}
