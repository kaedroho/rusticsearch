use std::str;

use kite::segment::Segment;
use kite::schema::FieldRef;
use kite::term::TermRef;
use kite::doc_id_set::DocIdSet;
use kite::statistics::Statistics;
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

    // TODO: Make statistics a parameter
    fn load_statistics(&self, stats: &mut Statistics) -> Result<(), String> {
        /// Converts statistic key strings "s1/total_docs" into tuples of 1 i32 and a Vec<u8> (1, ['t', 'o', 't', ...])
        fn parse_statistic_key(key: &[u8]) -> (u32, &[u8]) {
            let mut parts_iter = key[1..].split(|b| *b == b'/');
            let segment = str::from_utf8(parts_iter.next().unwrap()).unwrap().parse::<u32>().unwrap();
            let statistic_name = parts_iter.next().unwrap();

            (segment, statistic_name)
        }

        let kb = KeyBuilder::segment_stat_prefix(self.id);
        let mut iter = self.reader.snapshot.iterator();
        iter.seek(&kb.key());
        while iter.next() {
            let k = iter.key().unwrap();
            if k[0] != b's' {
                // No more statistics
                break;
            }

            let (segment, statistic_name) = parse_statistic_key(&k);

            if segment != self.id {
                // Segment finished
                break;
            }

            stats.increment_statistic(statistic_name, BigEndian::read_i64(&iter.value().unwrap()));
        }

        Ok(())
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
