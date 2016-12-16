use std::fmt;
use std::io::{Cursor, Read};

use roaring::{RoaringBitmap, Iter as RoaringBitmapIter};
use byteorder::{ByteOrder, BigEndian};


#[derive(Clone)]
pub struct DocIdSet {
    data: RoaringBitmap<u16>,
}


impl DocIdSet {
    pub fn new_filled(mut num_docs: u32) -> DocIdSet {
        let mut data: RoaringBitmap<u16> = RoaringBitmap::new();

        // Cap num_docs to 65536
        // Note: we cannot simply make num_docs a u16 as 65536 is a valid length
        if num_docs > 65536 {
            num_docs = 65536;
        }

        for doc_id in 0..num_docs {
            // Note: As num_docs is limited to 65536, doc_id cannot be greater than 65535
            data.insert(doc_id as u16);
        }

        DocIdSet {
            data: data
        }
    }

    pub fn from_bytes(data: Vec<u8>) -> DocIdSet {
        let mut roaring_data: RoaringBitmap<u16> = RoaringBitmap::new();
        let mut cursor = Cursor::new(data);

        loop {
            let mut buf = [0, 2];
            match cursor.read_exact(&mut buf) {
                Ok(()) => {
                    let doc_id = BigEndian::read_u16(&buf);
                    roaring_data.insert(doc_id);
                }
                Err(_) => break,
            }
        }

        DocIdSet {
            data: roaring_data
        }
    }

    pub fn iter<'a>(&'a self) -> DocIdSetIterator<'a> {
        DocIdSetIterator {
            inner: self.data.iter(),
        }
    }

    pub fn contains_doc(&self, doc_id: u16) -> bool {
        self.data.contains(doc_id)
    }

    pub fn union(&self, other: &DocIdSet) -> DocIdSet {
        let mut data: RoaringBitmap<u16> = self.data.clone();
        data.union_with(&other.data);

        DocIdSet {
            data: data
        }
    }

    pub fn intersection(&self, other: &DocIdSet) -> DocIdSet {
        let mut data: RoaringBitmap<u16> = self.data.clone();
        data.intersect_with(&other.data);

        DocIdSet {
            data: data
        }
    }

    pub fn exclusion(&self, other: &DocIdSet) -> DocIdSet {
        let mut data: RoaringBitmap<u16> = self.data.clone();
        data.difference_with(&other.data);

        DocIdSet {
            data: data
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
    inner: RoaringBitmapIter<'a, u16>,
}


impl<'a> Iterator for DocIdSetIterator<'a> {
    type Item = u16;

    fn next(&mut self) -> Option<u16> {
        self.inner.next()
    }
}
