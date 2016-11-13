use rocksdb;
use kite::schema::FieldRef;
use byteorder::{ByteOrder, BigEndian};

use RocksDBIndexReader;
use key_builder::KeyBuilder;
use doc_id_set::DocIdSet;
use term_dictionary::TermRef;
use document_index::DocRef;


#[derive(Debug)]
pub enum SegmentReadError {
    RocksDB(rocksdb::Error),
}


pub trait Segment {
    fn load_statistic(&self, stat_name: &[u8]) -> Result<Option<i64>, SegmentReadError>;
    fn load_stored_field_value_raw(&self, doc_ord: u16, field_ref: FieldRef, value_type: &[u8]) -> Result<Option<Vec<u8>>, SegmentReadError>;
    fn load_term_directory(&self, field_ref: FieldRef, term_ref: TermRef) -> Result<Option<DocIdSet>, SegmentReadError>;
    fn load_deletion_list(&self) -> Result<Option<DocIdSet>, SegmentReadError>;
    fn id(&self) -> u32;

    fn doc_ref(&self, ord: u16) -> DocRef {
        DocRef::from_segment_ord(self.id(), ord)
    }
}


pub struct RocksDBSegment<'a> {
    reader: &'a RocksDBIndexReader<'a>,
    id: u32,
}


impl<'a> RocksDBSegment<'a> {
    pub fn new(reader: &'a RocksDBIndexReader, id: u32) -> RocksDBSegment<'a> {
        RocksDBSegment {
            reader: reader,
            id: id,
        }
    }
}


impl<'a> Segment for RocksDBSegment<'a> {
    fn id(&self) -> u32 {
        self.id
    }

    fn load_statistic(&self, stat_name: &[u8]) -> Result<Option<i64>, SegmentReadError> {
        let kb = KeyBuilder::segment_stat(self.id, stat_name);
        let val = try!(self.reader.snapshot.get(&kb.key())).map(|val| BigEndian::read_i64(&val));
        Ok(val)
    }

    fn load_stored_field_value_raw(&self, doc_ord: u16, field_ref: FieldRef, value_type: &[u8]) -> Result<Option<Vec<u8>>, SegmentReadError> {
        let kb = KeyBuilder::stored_field_value(self.id, doc_ord, field_ref.ord(), value_type);
        let val = try!(self.reader.snapshot.get(&kb.key()));
        Ok(val.map(|v| v.to_vec()))
    }

    fn load_term_directory(&self, field_ref: FieldRef, term_ref: TermRef) -> Result<Option<DocIdSet>, SegmentReadError> {
        let kb = KeyBuilder::segment_dir_list(self.id, field_ref.ord(), term_ref.ord());
        let doc_id_set = try!(self.reader.snapshot.get(&kb.key())).map(|doc_id_set| DocIdSet::FromRDB(doc_id_set));
        Ok(doc_id_set)
    }

    fn load_deletion_list(&self) -> Result<Option<DocIdSet>, SegmentReadError> {
        let kb = KeyBuilder::segment_del_list(self.id);
        let doc_id_set = try!(self.reader.snapshot.get(&kb.key())).map(|doc_id_set| DocIdSet::FromRDB(doc_id_set));
        Ok(doc_id_set)
    }
}


impl From<rocksdb::Error> for SegmentReadError {
    fn from(e: rocksdb::Error) -> SegmentReadError {
        SegmentReadError::RocksDB(e)
    }
}
