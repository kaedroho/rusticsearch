use std::str;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::BTreeMap;

use rocksdb::{DB, Writable, WriteBatch, IteratorMode, Direction};
use kite::Term;
use kite::query::term_selector::TermSelector;
use byteorder::{ByteOrder, BigEndian};

use key_builder::KeyBuilder;


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct DocRef(u32, u16);


impl DocRef {
    pub fn chunk(&self) -> u32 {
        self.0
    }

    pub fn ord(&self) -> u16 {
        self.1
    }

    pub fn as_u64(&self) -> u64 {
        (self.0 as u64) << 16 | (self.1 as u64)
    }

    pub fn from_chunk_ord(chunk: u32, ord: u16) -> DocRef {
        DocRef(chunk, ord)
    }

    pub fn from_u64(val: u64) -> DocRef {
        let chunk = (val >> 16) & 0xFFFFFFFF;
        let ord = val & 0xFFFF;
        DocRef(chunk as u32, ord as u16)
    }
}


/// Manages the index's "document index"
pub struct DocumentIndexManager {
    primary_key_index: RwLock<BTreeMap<Vec<u8>, DocRef>>,
}


impl DocumentIndexManager {
    /// Generates a new document index
    pub fn new(db: &DB) -> DocumentIndexManager {
        DocumentIndexManager {
            primary_key_index: RwLock::new(BTreeMap::new()),
        }
    }

    /// Loads the document index from an index
    pub fn open(db: &DB) -> DocumentIndexManager {
        // Read primary key index
        let mut primary_key_index = BTreeMap::new();
        for (k, v) in db.iterator(IteratorMode::From(b"k", Direction::Forward)) {
            if k[0] != b'k' {
                break;
            }

            let chunk = BigEndian::read_u32(&v[0..4]);
            let ord = BigEndian::read_u16(&v[4..6]);
            let doc_ref = DocRef::from_chunk_ord(chunk, ord);

            primary_key_index.insert(k[1..].to_vec(), doc_ref);
        }

        DocumentIndexManager {
            primary_key_index: RwLock::new(primary_key_index),
        }
    }

    pub fn insert_or_replace_key(&self, db: &DB, key: &Vec<u8>, doc_ref: DocRef) -> Option<DocRef> {
        // Update primary_key_index
        let mut write_batch = WriteBatch::default();
        let previous_doc_ref = self.primary_key_index.write().unwrap().insert(key.clone(), doc_ref);

        let mut kb = KeyBuilder::primary_key_index(key);
        let mut doc_ref_bytes = [0; 6];
        BigEndian::write_u32(&mut doc_ref_bytes, doc_ref.chunk());
        BigEndian::write_u16(&mut doc_ref_bytes[4..], doc_ref.ord());
        write_batch.put(&kb.key(), &doc_ref_bytes);

        // If there was a document there previously, mark it as deleted
        if let Some(previous_doc_ref) = previous_doc_ref {
            let mut kb = KeyBuilder::chunk_del_list(previous_doc_ref.chunk());
            let mut previous_doc_id_bytes = [0; 2];
            BigEndian::write_u16(&mut previous_doc_id_bytes, previous_doc_ref.ord());
            write_batch.merge(&kb.key(), &previous_doc_id_bytes);

            // Increment deleted docs
            let mut kb = KeyBuilder::chunk_stat(previous_doc_ref.chunk(), b"deleted_docs");
            let mut inc_bytes = [0; 8];
            BigEndian::write_i64(&mut inc_bytes, 1);
            write_batch.merge(&kb.key(), &inc_bytes);
        }

        // Write document data
        db.write(write_batch);

        previous_doc_ref
    }
}
