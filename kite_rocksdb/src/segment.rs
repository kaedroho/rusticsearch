use std::str;
use std::sync::atomic::{AtomicUsize, Ordering};

use rocksdb::{self, DB, Writable, IteratorMode, Direction};
use rocksdb::rocksdb::{Snapshot, DBIterator};


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
    pub fn iter_active<'a>(&self, snapshot: &'a Snapshot) -> ActiveSegmentsIterator<'a> {
        ActiveSegmentsIterator {
            iter: snapshot.iterator(IteratorMode::From(b"a", Direction::Forward)),
            fused: false,
        }
    }
}


pub struct ActiveSegmentsIterator<'a> {
    iter: DBIterator<'a>,
    fused: bool,
}


impl<'a> Iterator for ActiveSegmentsIterator<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.fused {
            return None;
        }

        match self.iter.next() {
            Some((k, _)) => {
                if k[0] != b'a' {
                    self.fused = true;
                    return None;
                }

                Some(str::from_utf8(&k[1..]).unwrap().parse::<u32>().unwrap())
            }
            None => {
                self.fused = true;
                return None;
            }
        }
    }
}
