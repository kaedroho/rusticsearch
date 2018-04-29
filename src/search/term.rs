use chrono::{DateTime, Utc, Timelike};
use byteorder::{WriteBytesExt, LittleEndian};


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct TermId(pub u32);


#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub struct Term(Vec<u8>);

impl Term {
    pub fn from_bytes(bytes: &[u8]) -> Term {
        Term(bytes.to_vec())
    }

    pub fn from_string(string: &str) -> Term {
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
        bytes.write_i64::<LittleEndian>(value).unwrap();
        Term(bytes)
    }

    pub fn from_datetime(value: &DateTime<Utc>) -> Term {
        let mut bytes = Vec::with_capacity(0);
        let timestamp = value.timestamp();
        let micros = value.nanosecond() / 1000;
        let timestamp_with_micros = timestamp * 1000000 + micros as i64;
        bytes.write_i64::<LittleEndian>(timestamp_with_micros).unwrap();
        Term(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc, Timelike};
    use super::Term;

    #[test]
    fn test_string_to_bytes() {
        let term = Term::from_string("foo");

        assert_eq!(term.as_bytes().to_vec(), vec![102, 111, 111])
    }

    #[test]
    fn test_hiragana_string_to_bytes() {
        let term = Term::from_string("こんにちは");

        assert_eq!(term.as_bytes().to_vec(), vec![227, 129, 147, 227, 130, 147, 227, 129, 171, 227, 129, 161, 227, 129, 175])
    }

    #[test]
    fn test_blank_string_to_bytes() {
        let term = Term::from_string("");

        assert_eq!(term.as_bytes().to_vec(), vec![] as Vec<u8>)
    }

    #[test]
    fn test_boolean_true_to_bytes() {
        let term = Term::from_boolean(true);

        // 116 = 't' in ASCII
        assert_eq!(term.as_bytes().to_vec(), vec![116])
    }

    #[test]
    fn test_boolean_false_to_bytes() {
        let term = Term::from_boolean(false);

        // 102 = 'f' in ASCII
        assert_eq!(term.as_bytes().to_vec(), vec![102])
    }

    #[test]
    fn test_integer_to_bytes() {
        let term = Term::from_integer(123);

        assert_eq!(term.as_bytes().to_vec(), vec![123, 0, 0, 0, 0, 0, 0, 0])
    }

    #[test]
    fn test_negative_integer_to_bytes() {
        let term = Term::from_integer(-123);

        assert_eq!(term.as_bytes().to_vec(), vec![133, 255, 255, 255, 255, 255, 255, 255])
    }

    #[test]
    fn test_datetime_to_bytes() {
        let date = "2016-07-23T16:15:00+01:00".parse::<DateTime<Utc>>().unwrap();
        let term = Term::from_datetime(&date);

        assert_eq!(term.as_bytes().to_vec(), vec![0, 101, 191, 3, 79, 56, 5, 0])
    }

    #[test]
    fn test_datetime_with_microseconds_to_bytes() {
        let mut date = "2016-07-23T16:15:00+01:00".parse::<DateTime<Utc>>().unwrap();
        date = date.with_nanosecond(123123123).unwrap();
        let term = Term::from_datetime(&date);

        // This is exactly 123123 higher than the result of "test_datetime_to_bytes"
        assert_eq!(term.as_bytes().to_vec(), vec![243, 69, 193, 3, 79, 56, 5, 0])
    }

    #[test]
    fn test_datetime_with_different_timezone_to_bytes() {
        let date = "2016-07-23T16:15:00+02:00".parse::<DateTime<Utc>>().unwrap();
        let term = Term::from_datetime(&date);

        // This is exactly 3_600_000_000 lower than the result of "test_datetime_to_bytes"
        assert_eq!(term.as_bytes().to_vec(), vec![0, 193, 43, 45, 78, 56, 5, 0])
    }
}
