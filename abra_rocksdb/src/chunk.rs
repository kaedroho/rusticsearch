use std::sync::atomic::{AtomicU32, Ordering};

use rocksdb::{DB, Writable};


pub struct ChunkManager {
    next_chunk: AtomicU32,
}


impl ChunkManager {
    pub fn new(db: &DB) -> ChunkManager {
        // Next chunk
        db.put(b".next_chunk", b"1");

        ChunkManager {
            next_chunk: AtomicU32::new(1),
        }
    }

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

    pub fn new_chunk(&mut self, db: &DB) -> u32 {
        let next_chunk = self.next_chunk.fetch_add(1, Ordering::SeqCst);
        db.put(b".next_chunk", (next_chunk + 1).to_string().as_bytes());
        next_chunk
    }
}