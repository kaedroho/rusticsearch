#![feature(integer_atomics)]

extern crate abra;
extern crate rocksdb;
extern crate rustc_serialize;
extern crate byteorder;

use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::BTreeMap;

use rocksdb::{DB, Writable, Options, MergeOperands};
use abra::{Term, Document};
use abra::schema::{Schema, FieldType, FieldRef, AddFieldError};
use rustc_serialize::{json, Encodable};
use byteorder::{ByteOrder, BigEndian};


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct TermRef(u32);


impl TermRef {
    pub fn ord(&self) -> u32 {
        self.0
    }
}


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct DocRef(u32, u16);


impl DocRef {
    pub fn chunk(&self) -> u32 {
        self.0
    }

    pub fn ord(&self) -> u16 {
        self.1
    }
}


fn merge_keys(key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    match key[0] {
        b'd' => {
            // Directory (sequence of two byte document ids)

            // Allocate vec for new Value
            let new_size = match existing_val {
                Some(existing_val) => existing_val.len(),
                None => 0,
            } + operands.size_hint().0 * 2;

            let mut new_val = Vec::with_capacity(new_size);

            // Push existing value
            existing_val.map(|v| {
                for b in v {
                    new_val.push(*b);
                }
            });

            // Append new entries
            for op in operands {
                for b in op {
                    new_val.push(*b);
                }
            }

            new_val
        }
        _ => {
            // Unrecognised key, fallback to emulating a put operation (by taking the last value)
            operands.last().unwrap().iter().cloned().collect()
        }
    }
}


pub struct RocksDBIndexStore {
    schema: Arc<Schema>,
    db: DB,
    next_term_ref: AtomicU32,
    term_dictionary: RwLock<BTreeMap<Vec<u8>, TermRef>>,
    next_chunk: AtomicU32,
}


impl RocksDBIndexStore {
    pub fn create(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        opts.add_merge_operator("merge operator", merge_keys);
        opts.create_if_missing(true);
        let db = try!(DB::open(&opts, path));

        // Schema
        let schema = Schema::new();
        db.put(b".schema", json::encode(&schema).unwrap().as_bytes());

        // Next term ref
        db.put(b".next_term_ref", b"1");

        // Next chunk
        db.put(b".next_chunk", b"1");

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            next_term_ref: AtomicU32::new(1),
            term_dictionary: RwLock::new(BTreeMap::new()),
            next_chunk: AtomicU32::new(1),
        })
    }

    pub fn open(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        opts.add_merge_operator("merge operator", merge_keys);
        let db = try!(DB::open(&opts, path));

        let schema = match db.get(b".schema") {
            Ok(Some(schema)) => {
                let schema = schema.to_utf8().unwrap().to_string();
                json::decode(&schema).unwrap()
            }
            Ok(None) => Schema::new(),  // TODO: error
            Err(_) => Schema::new(),  // TODO: error
        };

        let next_term_ref = match db.get(b".next_term_ref") {
            Ok(Some(next_term_ref)) => {
                next_term_ref.to_utf8().unwrap().parse::<u32>().unwrap()
            }
            Ok(None) => 1,  // TODO: error
            Err(_) => 1,  // TODO: error
        };

        let next_chunk = match db.get(b".next_chunk") {
            Ok(Some(next_chunk)) => {
                next_chunk.to_utf8().unwrap().parse::<u32>().unwrap()
            }
            Ok(None) => 1,  // TODO: error
            Err(_) => 1,  // TODO: error
        };

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            next_term_ref: AtomicU32::new(next_term_ref),
            term_dictionary: RwLock::new(BTreeMap::new()),
            next_chunk: AtomicU32::new(next_chunk),
        })
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType) -> Result<FieldRef, AddFieldError> {
        let mut schema_copy = (*self.schema).clone();
        let field_ref = try!(schema_copy.add_field(name, field_type));
        self.schema = Arc::new(schema_copy);

        self.db.put(b".schema", json::encode(&self.schema).unwrap().as_bytes());

        Ok(field_ref)
    }

    pub fn remove_field(&mut self, field_ref: &FieldRef) -> bool {
        let mut schema_copy = (*self.schema).clone();
        let field_removed = schema_copy.remove_field(field_ref);

        if field_removed {
            self.schema = Arc::new(schema_copy);
            self.db.put(b".schema", json::encode(&self.schema).unwrap().as_bytes());
        }

        field_removed
    }

    fn get_or_create_term(&mut self, term: &Term) -> TermRef {
        let term_bytes = term.to_bytes();

        if let Some(term_ref) = self.term_dictionary.read().unwrap().get(&term_bytes) {
            return *term_ref;
        }

        // Term doesn't exist in the term dictionary

        // Increment next_term_ref
        let next_term_ref = self.next_term_ref.fetch_add(1, Ordering::SeqCst);
        self.db.put(b".next_term_ref", (next_term_ref + 1).to_string().as_bytes());

        // Create term ref
        let term_ref = TermRef(next_term_ref);

        // Get exclusive lock to term dictionary
        let mut term_dictionary = self.term_dictionary.write().unwrap();

        // It's possible that another thread has written the term to the dictionary
        // since we checked earlier. If this is the case, We should forget about
        // writing our TermRef and use the one that has been inserted already.
        if let Some(term_ref) = term_dictionary.get(&term_bytes) {
            return *term_ref;
        }

        // Write it to the on-disk term dictionary
        let mut key = Vec::with_capacity(1 + term_bytes.len());
        key.push(b't');
        for byte in term_bytes.iter() {
            key.push(*byte);
        }
        self.db.put(&key, next_term_ref.to_string().as_bytes());

        // Write it to the term dictionary
        term_dictionary.insert(term_bytes, term_ref);

        term_ref
    }

    pub fn insert_or_update_document(&mut self, doc: Document) {
        // Allocate a new chunk for the document

        // Chunk merges are very slow so we should avoid doing them at runtime
        // which is why each new document is created in a fresh chunk.
        // Later on, a background process will come and merge any small chunks
        // together.

        // For best performance, documents should be inserted in batches.

        // Increment next_chunk
        let next_chunk = self.next_chunk.fetch_add(1, Ordering::SeqCst);
        self.db.put(b".next_chunk", (next_chunk + 1).to_string().as_bytes());

        // Create doc ref
        let doc_ref = DocRef(next_chunk, 0);

        // Insert contents
        for (field_ref, tokens) in doc.fields.iter() {
            for token in tokens.iter() {
                let term_ref = self.get_or_create_term(&token.term);

                let mut key = Vec::with_capacity(20);
                key.push(b'd');
                for byte in field_ref.ord().to_string().as_bytes() {
                    key.push(*byte);
                }
                key.push(b'/');
                for byte in term_ref.ord().to_string().as_bytes() {
                    key.push(*byte);
                }
                key.push(b'/');
                for byte in doc_ref.chunk().to_string().as_bytes() {
                    key.push(*byte);
                }

                let mut doc_id_bytes = [0; 2];
                BigEndian::write_u16(&mut doc_id_bytes, doc_ref.ord());
                self.db.merge(&key, &doc_id_bytes);
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use rocksdb::{DB, Options};

    use super::RocksDBIndexStore;

    fn clean_test_indices() {
        use std::fs::remove_dir_all;

        remove_dir_all("test_indices");
    }

    #[test]
    fn test_create() {
        clean_test_indices();

        let store = RocksDBIndexStore::create("test_indices/test_create");
        assert!(store.is_ok());
    }

    #[test]
    fn test_open() {
        clean_test_indices();

        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_err());

        // Create DB
        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, "test_indices/test_open").unwrap();

        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_ok());
    }
}
