use std::io::Cursor;

use search::segment::{SegmentId, Segment};
use search::schema::FieldId;
use search::term::TermId;
use roaring::RoaringBitmap;
use byteorder::{ByteOrder, LittleEndian};

use super::RocksDBReader;
use super::key_builder::KeyBuilder;

pub struct RocksDBSegment<'a> {
    reader: &'a RocksDBReader<'a>,
    id: u32,
}

impl<'a> RocksDBSegment<'a> {
    pub fn new(reader: &'a RocksDBReader, id: u32) -> RocksDBSegment<'a> {
        RocksDBSegment {
            reader: reader,
            id: id,
        }
    }
}

impl<'a> Segment for RocksDBSegment<'a> {
    fn id(&self) -> SegmentId {
        SegmentId(self.id)
    }

    fn load_statistic(&self, stat_name: &[u8]) -> Result<Option<i64>, String> {
        let kb = KeyBuilder::segment_stat(self.id, stat_name);
        let val = try!(self.reader.snapshot.get(&kb.key())).map(|val| LittleEndian::read_i64(&val));
        Ok(val)
    }

    fn load_stored_field_value_raw(&self, doc_local_id: u16, field_id: FieldId, value_type: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let kb = KeyBuilder::stored_field_value(self.id, doc_local_id, field_id.0, value_type);
        let val = try!(self.reader.snapshot.get(&kb.key()));
        Ok(val.map(|v| v.to_vec()))
    }

    fn load_postings_list(&self, field_id: FieldId, term_id: TermId) -> Result<Option<RoaringBitmap>, String> {
        let kb = KeyBuilder::segment_postings_list(self.id, field_id.0, term_id.0);
        let doc_id_set = try!(self.reader.snapshot.get(&kb.key())).map(|doc_id_set| RoaringBitmap::deserialize_from(Cursor::new(&doc_id_set[..])).unwrap());
        Ok(doc_id_set)
    }

    fn load_deletion_list(&self) -> Result<Option<RoaringBitmap>, String> {
        let kb = KeyBuilder::segment_del_list(self.id);
        let doc_id_set = try!(self.reader.snapshot.get(&kb.key())).map(|doc_id_set| RoaringBitmap::deserialize_from(Cursor::new(&doc_id_set[..])).unwrap());
        Ok(doc_id_set)
    }
}
