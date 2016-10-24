use byteorder::{ByteOrder, BigEndian};

use {RocksDBIndexStore, RocksDBIndexReader};
use errors::RocksDBReadError;
use key_builder::KeyBuilder;


struct SegmentStatistics {
    total_docs: i64,
    deleted_docs: i64,
}


impl SegmentStatistics {
    fn read(reader: &RocksDBIndexReader, segment: u32) -> Result<SegmentStatistics, RocksDBReadError> {
        // Total docs
        let kb = KeyBuilder::segment_stat(segment, b"total_docs");

        let total_docs = match reader.snapshot.get(&kb.key()) {
            Ok(Some(val_bytes)) => {
                BigEndian::read_i64(&val_bytes)
            }
            Ok(None) => 0,
            Err(e) => return Err(RocksDBReadError::new(kb.key().to_vec(), e)),
        };

        // Deleted docs
        let kb = KeyBuilder::segment_stat(segment, b"deleted_docs");

        let deleted_docs = match reader.snapshot.get(&kb.key()) {
            Ok(Some(val_bytes)) => {
                BigEndian::read_i64(&val_bytes)
            }
            Ok(None) => 0,
            Err(e) => return Err(RocksDBReadError::new(kb.key().to_vec(), e)),
        };

        Ok(SegmentStatistics {
            total_docs: total_docs,
            deleted_docs: deleted_docs,
        })
    }
}


impl RocksDBIndexStore {
    fn get_segment_statistics(&self) -> Result<Vec<(u32, SegmentStatistics)>, RocksDBReadError> {
        let mut segment_stats = Vec::new();
        let reader = self.reader();

        for segment in self.segments.iter_active(&reader.snapshot) {
            let stats = try!(SegmentStatistics::read(&reader, segment));
            segment_stats.push((segment, stats));
        }

        Ok(segment_stats)
    }

    /// Run a maintenance task on the index
    /// This must be run periodically by a background thread. It is not currently thread-safe
    pub fn run_maintenance_task(&self) -> Result<(), String> {
        let segment_stats = try!(self.get_segment_statistics());

        Ok(())
    }
}
