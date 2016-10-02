use std::fmt;
use std::io::{Cursor, Read, Write};
use std::collections::HashMap;
use std::rc::Rc;

use abra::schema::{FieldRef, SchemaRead};
use abra::query::Query;
use abra::query::term_scorer::TermScorer;
use abra::collectors::{Collector, DocumentMatch};
use rocksdb::DBVector;
use byteorder::{ByteOrder, BigEndian};

use key_builder::KeyBuilder;
use term_dictionary::TermRef;
use super::super::{RocksDBIndexReader, DocRef};


pub enum DocIdSet {
    Owned(Vec<u8>),
    FromRDB(DBVector),
}


impl DocIdSet {
    pub fn new_filled(num_docs: u16) -> DocIdSet {
        let mut data: Vec<u8> = Vec::new();

        for doc_id in 0..num_docs {
            let mut doc_id_bytes = [0; 2];
            BigEndian::write_u16(&mut doc_id_bytes, doc_id);

            data.push(doc_id_bytes[0]);
            data.push(doc_id_bytes[1]);
        }

        DocIdSet::Owned(data)
    }

    pub fn get_cursor(&self) -> Cursor<&[u8]> {
        match *self {
            DocIdSet::Owned(ref data) => {
                Cursor::new(&data[..])
            }
            DocIdSet::FromRDB(ref data) => {
                Cursor::new(&data[..])
            }
        }
    }

    pub fn iter<'a>(&'a self) -> DocIdSetIterator<'a> {
        DocIdSetIterator {
            cursor: self.get_cursor(),
        }
    }

    pub fn contains_doc(&self, doc_id: u16) -> bool {
        // TODO: optimise
        for d in self.iter() {
            if d == doc_id {
                return true;
            }
        }

        false
    }

    pub fn union(&self, other: &DocIdSet) -> DocIdSet {
        // TODO: optimise
        let mut data: Vec<u8> = Vec::new();

        let mut a = self.iter().peekable();
        let mut b = other.iter().peekable();

        loop {
            let mut next_a = false;
            let mut next_b = false;

            match (a.peek(), b.peek()) {
                (Some(a_doc), Some(b_doc)) => {
                    let mut doc_id_bytes = [0; 2];
                    BigEndian::write_u16(&mut doc_id_bytes, *a_doc);

                    data.push(doc_id_bytes[0]);
                    data.push(doc_id_bytes[1]);

                    if a_doc == b_doc {
                        next_a = true;
                        next_b = true;
                    } else if a_doc > b_doc {
                        next_b = true;
                    } else if a_doc < b_doc {
                        next_a = true;
                    }
                }
                (Some(a_doc), None) => {
                    let mut doc_id_bytes = [0; 2];
                    BigEndian::write_u16(&mut doc_id_bytes, *a_doc);

                    data.push(doc_id_bytes[0]);
                    data.push(doc_id_bytes[1]);

                    next_a = true;
                }
                (None, Some(b_doc)) => {
                    let mut doc_id_bytes = [0; 2];
                    BigEndian::write_u16(&mut doc_id_bytes, *b_doc);

                    data.push(doc_id_bytes[0]);
                    data.push(doc_id_bytes[1]);

                    next_b = true;
                }
                (None, None) => break
            }

            if next_a {
                a.next();
            }

            if next_b {
                b.next();
            }
        }

        DocIdSet::Owned(data)
    }

    pub fn intersection(&self, other: &DocIdSet) -> DocIdSet {
        // TODO: optimise
        let mut data: Vec<u8> = Vec::new();

        let mut a = self.iter().peekable();
        let mut b = other.iter().peekable();

        loop {
            let a_doc = match a.peek() {
                Some(a) => *a,
                None => break,
            };
            let b_doc = match b.peek() {
                Some(b) => *b,
                None => break,
            };

            if a_doc == b_doc {
                let mut doc_id_bytes = [0; 2];
                BigEndian::write_u16(&mut doc_id_bytes, a_doc);

                data.push(doc_id_bytes[0]);
                data.push(doc_id_bytes[1]);

                a.next();
                b.next();
            } else if a_doc > b_doc {
                b.next();
            } else if a_doc < b_doc {
                a.next();
            }
        }

        DocIdSet::Owned(data)
    }

    pub fn exclusion(&self, other: &DocIdSet) -> DocIdSet {
        // TODO: optimise
        let mut data: Vec<u8> = Vec::new();

        let mut a = self.iter().peekable();
        let mut b = other.iter().peekable();

        loop {
            let a_doc = match a.peek() {
                Some(a) => *a,
                None => break,
            };
            let b_doc = match b.peek() {
                Some(b) => *b,
                None => {
                    let mut doc_id_bytes = [0; 2];
                    BigEndian::write_u16(&mut doc_id_bytes, a_doc);

                    data.push(doc_id_bytes[0]);
                    data.push(doc_id_bytes[1]);

                    a.next();

                    continue;
                },
            };

            if a_doc == b_doc {
                a.next();
                b.next();
            } else if a_doc > b_doc {
                b.next();
            } else if a_doc < b_doc {
                let mut doc_id_bytes = [0; 2];
                BigEndian::write_u16(&mut doc_id_bytes, a_doc);

                data.push(doc_id_bytes[0]);
                data.push(doc_id_bytes[1]);

                a.next();
            }
        }

        DocIdSet::Owned(data)
    }
}


impl Clone for DocIdSet {
    fn clone(&self) -> DocIdSet {
        match *self {
            DocIdSet::Owned(ref data) => {
                DocIdSet::Owned(data.clone())
            }
            DocIdSet::FromRDB(ref data) => {
                let mut new_data = Vec::with_capacity(data.len());
                new_data.write_all(data);
                DocIdSet::Owned(new_data)
            }
        }
    }
}


impl fmt::Debug for DocIdSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut iterator = self.iter();

        try!(write!(f, "["));

        let first_item = iterator.next();
        if let Some(first_item) = first_item {
            try!(write!(f, "{:?}", first_item));
        }

        for item in iterator {
            try!(write!(f, ", {:?}", item));
        }

        write!(f, "]")
    }
}


pub struct DocIdSetIterator<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> Iterator for DocIdSetIterator<'a> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        let mut buf = [0, 2];
        match self.cursor.read_exact(&mut buf) {
            Ok(()) => {
                Some(BigEndian::read_u16(&buf))
            }
            Err(_) => None
        }
    }
}
