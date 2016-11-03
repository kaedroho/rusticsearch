use rocksdb;
use kite::schema::FieldRef;
use byteorder::{ByteOrder, BigEndian};

use RocksDBIndexReader;
use key_builder::KeyBuilder;
use doc_id_set::DocIdSet;
use term_dictionary::TermRef;
use document_index::DocRef;


pub struct Segment<'a> {
    reader: &'a RocksDBIndexReader<'a>,
    id: u32,
}


impl<'a> Segment<'a> {
    pub fn new(reader: &'a RocksDBIndexReader, id: u32) -> Segment<'a> {
        Segment {
            reader: reader,
            id: id,
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn doc_ref(&self, ord: u16) -> DocRef {
        DocRef::from_segment_ord(self.id, ord)
    }

    pub fn load_statistic(&self, stat_name: &[u8]) -> Result<Option<i64>, rocksdb::Error> {
        let kb = KeyBuilder::segment_stat(self.id, stat_name);
        let val = try!(self.reader.snapshot.get(&kb.key())).map(|val| BigEndian::read_i64(&val));
        Ok(val)
    }

    pub fn load_stored_field_value_raw(&self, doc_ord: u16, field_ref: FieldRef, value_type: &[u8]) -> Result<Option<rocksdb::DBVector>, rocksdb::Error> {
        let kb = KeyBuilder::stored_field_value(self.id, doc_ord, field_ref.ord(), value_type);
        let val = try!(self.reader.snapshot.get(&kb.key()));
        Ok(val)
    }

    pub fn load_term_directory(&self, field_ref: FieldRef, term_ref: TermRef) -> Result<Option<DocIdSet>, rocksdb::Error> {
        let kb = KeyBuilder::segment_dir_list(self.id, field_ref.ord(), term_ref.ord());
        let doc_id_set = try!(self.reader.snapshot.get(&kb.key())).map(|doc_id_set| DocIdSet::FromRDB(doc_id_set));
        Ok(doc_id_set)
    }

    pub fn load_deletion_list(&self) -> Result<Option<DocIdSet>, rocksdb::Error> {
        let kb = KeyBuilder::segment_del_list(self.id);
        let doc_id_set = try!(self.reader.snapshot.get(&kb.key())).map(|doc_id_set| DocIdSet::FromRDB(doc_id_set));
        Ok(doc_id_set)
    }
}
