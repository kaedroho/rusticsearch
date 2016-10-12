use std::str;
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::HashMap;

use rocksdb::{DB, Writable, DBIterator, IteratorMode, Direction};
use rocksdb::rocksdb::Snapshot;
use byteorder::{ByteOrder, BigEndian, WriteBytesExt};

use document_index::DocRef;
use key_builder::KeyBuilder;
use search::doc_id_set::DocIdSet;


#[derive(Debug)]
pub enum ChunkMergeError {
    TooManyDocs,
}


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
    pub fn new_chunk(&mut self, db: &DB) -> u32 {
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

    pub fn merge_chunks(&mut self, db: &DB, source_chunks: Vec<u32>) -> Result<u32, ChunkMergeError> {
        let mut dest_chunk = self.new_chunk(db);

        // Generate a mapping between the ids of the documents in the old chunks to the new one
        // This packs the id spaces of the old chunks together:
        // For example, say we have to merge 3 chunks with 100 documents each:
        //  - The first chunk's ids will be the same as before
        //  - The second chunk's ids will be remapped to 100 - 199
        //  - The third chunk's ids will be remapped to 200 - 299

        let mut doc_ref_mapping: HashMap<DocRef, u16> = HashMap::new();
        let mut current_ord: u32 = 0;

        for source_chunk in source_chunks.iter() {
            let mut kb = KeyBuilder::chunk_stat(*source_chunk, b"total_docs");
            let total_docs = match db.get(&kb.key()) {
                Ok(Some(total_docs_bytes)) => {
                    BigEndian::read_i64(&total_docs_bytes)
                }
                Ok(None) => continue,
                Err(e) => continue,  // TODO: Error
            };

            for source_ord in 0..total_docs {
                if current_ord >= 65536 {
                    return Err(ChunkMergeError::TooManyDocs);
                }

                let from = DocRef::from_chunk_ord(*source_chunk, source_ord as u16);
                doc_ref_mapping.insert(from, current_ord as u16);
                current_ord += 1;
            }
        }

        // Merge the term directories
        // The term directory keys are ordered to be most convenient for retrieving all the chunks
        // of for a term/field combination in one go (field/term/chunk). So we don't end up pulling
        // in a lot of unwanted data, we firstly iterate the keys, it they one of the source chunks
        // looking for then we load them and append them to our new chunk.

        /// Converts term directory key strings "d1/2/3" into tuples of 3 i32s (1, 2, 3)
        fn parse_term_directory_key(key: &[u8]) -> (u32, u32, u32) {
            let mut nums_iter = key[1..].split(|b| *b == b'/').map(|s| str::from_utf8(s).unwrap().parse::<u32>().unwrap());
            (nums_iter.next().unwrap(), nums_iter.next().unwrap(), nums_iter.next().unwrap())
        }

        let mut current_td_key: Option<(u32, u32)> = None;
        let mut current_td = Vec::new();

        for k in db.keys_iterator(IteratorMode::From(b"d", Direction::Forward)) {
            if k[0] != b'd' {
                // No more term directories to merge
                break;
            }

            let (field, term, chunk) = parse_term_directory_key(&k);

            if !source_chunks.contains(&chunk) {
                continue;
            }

            if current_td_key != Some((field, term)) {
                // Finished current term directory. Write it to the DB and start the next one
                if let Some((field, term)) = current_td_key {
                    let kb = KeyBuilder::chunk_dir_list(dest_chunk, field, term);
                    db.put(&kb.key(), &current_td);
                    current_td.clear();
                }

                current_td_key = Some((field, term));
            }

            // Merge term directory into the new one (and remap the doc ids)
            match db.get(&k) {
                Ok(Some(docid_set)) => {
                    for doc_id in DocIdSet::FromRDB(docid_set).iter() {
                        let doc_ref = DocRef::from_chunk_ord(chunk, doc_id);
                        let new_doc_id = doc_ref_mapping.get(&doc_ref).unwrap();
                        current_td.write_u16::<BigEndian>(*new_doc_id);
                    }
                }
                Ok(None) => {},  // FIXME
                Err(e) => {},  // FIXME
            }
        }

        // All done, write the last term directory
        if let Some((field, term)) = current_td_key {
            let kb = KeyBuilder::chunk_dir_list(dest_chunk, field, term);
            db.put(&kb.key(), &current_td);
            current_td.clear();
        }

        // Merge the stored values
        // All stored value keys start with the chunk id. So we need to:
        // - Iterate all stored value keys that are prefixed by one of the stored chunk ids
        // - Remap their doc ids to the one in the new chunk
        // - Write the value back with the new chunk/doc ids in the key

        // TODO

        // Merge the statistics
        // Like stored values, these start with chunk ids. But instead of just rewriting the
        // key, we need to sum up all the statistics across the chunks being merged.

        // TODO

        // Note: Don't merge the deletion lists
        // Deletion lists can change at any time so we must lock the "document index"
        // before merging them so they can't be altered during merge. we
        // cannot lock this until the commit phase though.

        Ok(dest_chunk)
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
