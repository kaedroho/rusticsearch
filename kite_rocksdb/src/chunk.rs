use std::str;
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::HashMap;

use rocksdb::{DB, Writable, DBIterator, IteratorMode, Direction};
use rocksdb::rocksdb::Snapshot;
use byteorder::{ByteOrder, BigEndian, WriteBytesExt};

use document_index::DocRef;
use key_builder::KeyBuilder;
use search::doc_id_set::DocIdSet;


/// Manages "chunks" within the index
///
/// The index is partitioned into immutable chunks. This manager is responsible
/// for allocating chunks keeping track of which chunks are active and
/// controlling routine tasks such as merging and vacuuming
pub struct ChunkManager {
    next_chunk: AtomicU32,
}


impl ChunkManager {
    /// Generates a new chunk manager
    pub fn new(db: &DB) -> ChunkManager {
        // TODO: Raise error if .next_chunk already exists
        // Next chunk
        db.put(b".next_chunk", b"1");

        ChunkManager {
            next_chunk: AtomicU32::new(1),
        }
    }

    /// Loads the chunk manager from an index
    pub fn open(db: &DB) -> ChunkManager {
        let next_chunk = match db.get(b".next_chunk") {
            Ok(Some(next_chunk)) => {
                next_chunk.to_utf8().unwrap().parse::<u32>().unwrap()
            }
            Ok(None) => 1,  // TODO: error
            Err(_) => 1,  // TODO: error
        };

        ChunkManager {
            next_chunk: AtomicU32::new(next_chunk),
        }
    }

    /// Allocates a new (inactive) chunk
    pub fn new_chunk(&self, db: &DB) -> u32 {
        let next_chunk = self.next_chunk.fetch_add(1, Ordering::SeqCst);
        db.put(b".next_chunk", (next_chunk + 1).to_string().as_bytes());
        next_chunk
    }

    /// Iterates currently active chunks
    pub fn iter_active<'a>(&self, snapshot: &'a Snapshot) -> ActiveChunksIterator {
        ActiveChunksIterator {
            iter: snapshot.iterator(IteratorMode::From(b"a", Direction::Forward)),
            fused: false,
        }
    }
}


pub struct ActiveChunksIterator {
    iter: DBIterator,
    fused: bool,
}


impl Iterator for ActiveChunksIterator {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.fused {
            return None;
        }

        match self.iter.next() {
            Some((k, v)) => {
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
