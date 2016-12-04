use kite::segment::Segment;
use kite::schema::FieldRef;
use kite::term::TermRef;
use kite::doc_id_set::DocIdSet;
use byteorder::{ByteOrder, BigEndian};

use RocksDBIndexReader;
use key_builder::KeyBuilder;


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

    fn load_statistic(&self, stat_name: &[u8]) -> Result<Option<i64>, String> {
        let kb = KeyBuilder::segment_stat(self.id, stat_name);
        let val = try!(self.reader.snapshot.get(&kb.key())).map(|val| BigEndian::read_i64(&val));
        Ok(val)
    }

    fn load_stored_field_value_raw(&self, doc_ord: u16, field_ref: FieldRef, value_type: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let kb = KeyBuilder::stored_field_value(self.id, doc_ord, field_ref.ord(), value_type);
        let val = try!(self.reader.snapshot.get(&kb.key()));
        Ok(val.map(|v| v.to_vec()))
    }

    fn load_term_directory(&self, field_ref: FieldRef, term_ref: TermRef) -> Result<Option<DocIdSet>, String> {
        let kb = KeyBuilder::segment_dir_list(self.id, field_ref.ord(), term_ref.ord());
        let doc_id_set = try!(self.reader.snapshot.get(&kb.key())).map(|doc_id_set| DocIdSet::from_bytes(doc_id_set.to_vec()));
        Ok(doc_id_set)
    }

    fn load_deletion_list(&self) -> Result<Option<DocIdSet>, String> {
        let kb = KeyBuilder::segment_del_list(self.id);
        let doc_id_set = try!(self.reader.snapshot.get(&kb.key())).map(|doc_id_set| DocIdSet::from_bytes(doc_id_set.to_vec()));
        Ok(doc_id_set)
    }
}
