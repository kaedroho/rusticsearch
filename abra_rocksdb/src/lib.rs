#![feature(integer_atomics)]

extern crate abra;
extern crate rocksdb;
extern crate rustc_serialize;

use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::BTreeMap;

use rocksdb::{DB, Writable, Options};
use abra::{Term, Document};
use abra::schema::{Schema, FieldType, FieldRef, AddFieldError};
use rustc_serialize::{json, Encodable};


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct TermRef(u32);


pub struct RocksDBIndexStore {
    schema: Arc<Schema>,
    db: DB,
    next_term_ref: AtomicU32,
    term_dictionary: RwLock<BTreeMap<Vec<u8>, TermRef>>,
}


impl RocksDBIndexStore {
    pub fn create(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = try!(DB::open(&opts, path));

        // Schema
        let schema = Schema::new();
        db.put(b".schema", json::encode(&schema).unwrap().as_bytes());

        // Next term ref
        db.put(b".next_term_ref", b"1");

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            next_term_ref: AtomicU32::new(1),
            term_dictionary: RwLock::new(BTreeMap::new()),
        })
    }

    pub fn open(path: &str) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
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

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            next_term_ref: AtomicU32::new(next_term_ref),
            term_dictionary: RwLock::new(BTreeMap::new()),
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
        // Put field contents in inverted index
        for (field_ref, tokens) in doc.fields.iter() {
            for token in tokens.iter() {
                let term_ref = self.get_or_create_term(&token.term);
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
