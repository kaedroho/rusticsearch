use byteorder::{ByteOrder, BigEndian};

use {RocksDBIndexStore, RocksDBIndexReader};
use errors::RocksDBReadError;
use key_builder::KeyBuilder;


#[derive(Debug)]
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
        let mut segment_stats = try!(self.get_segment_statistics());

        // TODO: Deactivate segments with 100% deletions
        // TODO: Vacuum segments with many deletions
        // TODO: Purge inactive segments

        // Merge segments

        // Firstly we classify each active segment into one of 5 groups (based on the number of
        // total documents they have):
        // Group 1: 1 - 9 docs
        // Group 2: 10 - 99 docs
        // Group 3: 100 - 999 docs
        // Group 4: 1000 - 9999 docs
        // Group 5: 10000 - 65536 docs

        // The group with the most active segments can perform a merge. A merge can be done on
        // between 5 - 1000 segments at a time. The smallest segments get merged first.

        // Sort by total_docs in ascending order
        // TODO: Check that this is ascending order
        segment_stats.sort_by_key(|&(_, ref stats)| stats.total_docs);

        let mut segments_g1 = Vec::new();
        let mut segments_g2 = Vec::new();
        let mut segments_g3 = Vec::new();
        let mut segments_g4 = Vec::new();
        let mut segments_g5 = Vec::new();

        for (segment, stats) in segment_stats {
            match stats.total_docs {
                1 ... 9 => segments_g1.push(segment),
                10 ... 99 => segments_g2.push(segment),
                100 ... 999 => segments_g3.push(segment),
                1000 ... 9999 => segments_g4.push(segment),
                10000 ... 65536 => segments_g5.push(segment),
                _ => {},
            }
        }

        // Now sort the groups by length in ascending order
        let mut segments_grouped = vec![segments_g1, segments_g2, segments_g3, segments_g4, segments_g5];
        segments_grouped.sort_by_key(|group| group.len());

        // The group with the most segments is our merge candidate. Check that it has above the
        // minimum number of documents to start a merge and truncate it to be less than the maximum.
        let mut group_to_merge = segments_grouped.pop().unwrap();

        if group_to_merge.len() >= 5 {
            // TODO: Check that we're not merging over 65536 docs
            group_to_merge.truncate(1000);
            try!(self.merge_segments(&group_to_merge));
            try!(self.purge_segments(&group_to_merge));
        }

        Ok(())
    }
}
