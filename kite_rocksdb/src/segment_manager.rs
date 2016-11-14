use std::str;
use std::sync::atomic::{AtomicUsize, Ordering};

use rocksdb::{self, DB, DBIterator};

use RocksDBIndexReader;
use segment::RocksDBSegment;


/// Manages "segments" within the index
///
/// The index is partitioned into immutable segments. This manager is responsible
/// for allocating segments keeping track of which segments are active and
/// controlling routine tasks such as merging and vacuuming
pub struct SegmentManager {
    next_segment: AtomicUsize,
}


impl SegmentManager {
    /// Generates a new segment manager
    pub fn new(db: &DB) -> Result<SegmentManager, rocksdb::Error> {
        // TODO: Raise error if .next_segment already exists
        // Next segment
        try!(db.put(b".next_segment", b"1"));

        Ok(SegmentManager {
            next_segment: AtomicUsize::new(1),
        })
    }

    /// Loads the segment manager from an index
    pub fn open(db: &DB) -> Result<SegmentManager, rocksdb::Error> {
        let next_segment = match try!(db.get(b".next_segment")) {
            Some(next_segment) => {
                next_segment.to_utf8().unwrap().parse::<u32>().unwrap()
            }
            None => 1,  // TODO: error
        };

        Ok(SegmentManager {
            next_segment: AtomicUsize::new(next_segment as usize),
        })
    }

    /// Allocates a new (inactive) segment
    pub fn new_segment(&self, db: &DB) -> Result<u32, rocksdb::Error> {
        let next_segment = self.next_segment.fetch_add(1, Ordering::SeqCst) as u32;
        try!(db.put(b".next_segment", (next_segment + 1).to_string().as_bytes()));
        Ok(next_segment)
    }

    /// Iterates currently active segments
    pub fn iter_active<'a>(&self, reader: &'a RocksDBIndexReader) -> ActiveSegmentsIterator<'a> {
        let mut iter = reader.snapshot.iterator();
        iter.seek(b"a");
        ActiveSegmentsIterator {
            reader: reader,
            iter: iter,
            fused: false,
        }
    }
}


pub struct ActiveSegmentsIterator<'a> {
    reader: &'a RocksDBIndexReader<'a>,
    iter: DBIterator,
    fused: bool,
}


impl<'a> Iterator for ActiveSegmentsIterator<'a> {
    type Item = RocksDBSegment<'a>;

    fn next(&mut self) -> Option<RocksDBSegment<'a>> {
        if !self.fused && self.iter.valid() {
            let segment_id = {
                let k = self.iter.key().unwrap();

                if k[0] != b'a' {
                    self.fused = true;
                    return None;
                }

                str::from_utf8(&k[1..]).unwrap().parse::<u32>().unwrap()
            };

            self.iter.next();

            Some(RocksDBSegment::new(self.reader, segment_id))
        } else {
            None
        }
    }
}
