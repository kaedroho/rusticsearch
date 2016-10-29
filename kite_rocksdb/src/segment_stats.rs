use byteorder::{ByteOrder, BigEndian};
use rocksdb;

use {RocksDBIndexStore, RocksDBIndexReader};
use key_builder::KeyBuilder;


#[derive(Debug)]
pub struct SegmentStatistics {
    total_docs: i64,
    deleted_docs: i64,
}


impl SegmentStatistics {
    fn read(reader: &RocksDBIndexReader, segment: u32) -> Result<SegmentStatistics, rocksdb::Error> {
        // Total docs
        let kb = KeyBuilder::segment_stat(segment, b"total_docs");

        let total_docs = match try!(reader.snapshot.get(&kb.key())) {
            Some(val_bytes) => {
                BigEndian::read_i64(&val_bytes)
            }
            None => 0,
        };

        // Deleted docs
        let kb = KeyBuilder::segment_stat(segment, b"deleted_docs");

        let deleted_docs = match try!(reader.snapshot.get(&kb.key())) {
            Some(val_bytes) => {
                BigEndian::read_i64(&val_bytes)
            }
            None => 0,
        };

        Ok(SegmentStatistics {
            total_docs: total_docs,
            deleted_docs: deleted_docs,
        })
    }

    #[inline]
    pub fn total_docs(&self) -> i64 {
        self.total_docs
    }

    #[inline]
    pub fn deleted_docs(&self) -> i64 {
        self.deleted_docs
    }
}


impl RocksDBIndexStore {
    pub fn get_segment_statistics(&self) -> Result<Vec<(u32, SegmentStatistics)>, rocksdb::Error> {
        let mut segment_stats = Vec::new();
        let reader = self.reader();

        for segment in self.segments.iter_active(&reader.snapshot) {
            let stats = try!(SegmentStatistics::read(&reader, segment));
            segment_stats.push((segment, stats));
        }

        Ok(segment_stats)
    }
}
