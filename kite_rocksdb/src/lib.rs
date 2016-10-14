#![feature(integer_atomics)]
#![feature(slice_patterns)]

extern crate kite;
extern crate rocksdb;
extern crate rustc_serialize;
#[macro_use]
extern crate maplit;
extern crate byteorder;
extern crate chrono;

pub mod key_builder;
pub mod chunk;
pub mod chunk_merge;
pub mod term_dictionary;
pub mod document_index;
pub mod search;

use std::str;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::BTreeMap;

use rocksdb::{DB, WriteBatch, Writable, Options, MergeOperands};
use rocksdb::rocksdb::Snapshot;
use kite::{Term, Document};
use kite::document::FieldValue;
use kite::schema::{Schema, SchemaRead, SchemaWrite, FieldType, FieldFlags, FieldRef, AddFieldError};
use rustc_serialize::{json, Encodable};
use byteorder::{ByteOrder, BigEndian};
use chrono::{NaiveDateTime, DateTime, UTC};

use key_builder::KeyBuilder;
use chunk::ChunkManager;
use term_dictionary::{TermDictionaryManager, TermRef};
use document_index::{DocumentIndexManager, DocRef};


fn merge_keys(key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    match key[0] {
        b'd' | b'x' => {
            // Sequence of two byte document ids
            // d = directory
            // x = deletion list

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
        b's' => {
            // Statistic
            // An i64 number that can be incremented or decremented
            let mut value = match existing_val {
                Some(existing_val) => BigEndian::read_i64(existing_val),
                None => 0
            };

            for op in operands {
                value += BigEndian::read_i64(op);
            }

            let mut buf = [0; 8];
            BigEndian::write_i64(&mut buf, value);
            buf.iter().cloned().collect()
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
    term_dictionary: TermDictionaryManager,
    chunks: ChunkManager,
    document_index: DocumentIndexManager,
    doc_key_mapping: RwLock<BTreeMap<Vec<u8>, DocRef>>,
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

        // Chunk manager
        let chunks = ChunkManager::new(&db);

        // Term dictionary manager
        let term_dictionary = TermDictionaryManager::new(&db);

        // Document index
        let document_index = DocumentIndexManager::new(&db);

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            term_dictionary: term_dictionary,
            chunks: chunks,
            document_index: document_index,
            doc_key_mapping: RwLock::new(BTreeMap::new()),
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

        // Chunk manager
        let chunks = ChunkManager::open(&db);

        // Term dictionary manager
        let term_dictionary = TermDictionaryManager::open(&db);

        // Document index
        let document_index = DocumentIndexManager::open(&db);

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            term_dictionary: term_dictionary,
            chunks: chunks,
            document_index: document_index,
            doc_key_mapping: RwLock::new(BTreeMap::new()),
        })
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType, field_flags: FieldFlags) -> Result<FieldRef, AddFieldError> {
        let mut schema_copy = (*self.schema).clone();
        let field_ref = try!(schema_copy.add_field(name, field_type, field_flags));
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

    pub fn insert_or_update_document(&self, doc: Document) {
        // Allocate a new chunk for the document
        // Chunk merges are very slow so we should avoid doing them at runtime
        // which is why each new document is created in a fresh chunk.
        // Later on, a background process will come and merge any small chunks
        // together. (For best performance, documents should be
        // inserted/updated in batches)
        let chunk = self.chunks.new_chunk(&self.db);

        // Create doc ref
        let doc_ref = DocRef::from_chunk_ord(chunk, 0);

        // Start write batch
        let mut write_batch = WriteBatch::default();

        // Set chunk active flag, this will activate the chunk as soon as the
        // write batch is written
        let mut kb = KeyBuilder::chunk_active(doc_ref.chunk());
        write_batch.put(&kb.key(), b"");

        // Insert contents

        // Indexed fields
        let mut token_count: i64 = 0;
        for (field_name, tokens) in doc.indexed_fields.iter() {
            let field_ref = match self.schema.get_field_by_name(field_name) {
                Some(field_ref) => field_ref,
                None => {
                    // TODO: error?
                    continue;
                }
            };

            for token in tokens.iter() {
                token_count += 1;
                let term_ref = self.term_dictionary.get_or_create(&self.db, &token.term);

                // Write directory list
                let mut kb = KeyBuilder::chunk_dir_list(doc_ref.chunk(), field_ref.ord(), term_ref.ord());
                let mut doc_id_bytes = [0; 2];
                BigEndian::write_u16(&mut doc_id_bytes, doc_ref.ord());
                write_batch.merge(&kb.key(), &doc_id_bytes);
            }
        }

        // Stored fields
        for (field_name, value) in doc.stored_fields.iter() {
            let field_ref = match self.schema.get_field_by_name(field_name) {
                Some(field_ref) => field_ref,
                None => {
                    // TODO: error?
                    continue;
                }
            };

            let mut kb = KeyBuilder::stored_field_value(doc_ref.chunk(), doc_ref.ord(), field_ref.ord());
            write_batch.merge(&kb.key(), &value.to_bytes());
        }

        // Increment total docs
        let mut kb = KeyBuilder::chunk_stat(doc_ref.chunk(), b"total_docs");
        let mut inc_bytes = [0; 8];
        BigEndian::write_i64(&mut inc_bytes, 1);
        write_batch.merge(&kb.key(), &inc_bytes);

        // Increment total tokens
        let mut kb = KeyBuilder::chunk_stat(doc_ref.chunk(), b"total_tokens");
        let mut inc_bytes = [0; 8];
        BigEndian::write_i64(&mut inc_bytes, token_count);
        write_batch.merge(&kb.key(), &inc_bytes);

        // Write document data
        self.db.write(write_batch);

        // Update document index
        self.document_index.insert_or_replace_key(&self.db, &doc.key.as_bytes().iter().cloned().collect(), doc_ref);
    }

    pub fn remove_document_by_key(&mut self, doc_key: &str) -> bool {
        match self.document_index.delete_document_by_key(&self.db, &doc_key.as_bytes().iter().cloned().collect()) {
            Some(doc_ref) => true,
            None => false,
        }
    }

    pub fn reader<'a>(&'a self) -> RocksDBIndexReader<'a> {
        RocksDBIndexReader {
            store: &self,
            snapshot: self.db.snapshot(),
        }
    }
}


pub struct RocksDBIndexReader<'a> {
    store: &'a RocksDBIndexStore,
    snapshot: Snapshot<'a>
}


impl<'a> RocksDBIndexReader<'a> {
    pub fn schema(&self) -> &Schema {
        &self.store.schema
    }

    pub fn contains_document_key(&self, doc_key: &str) -> bool {
        // TODO: use snapshot
        self.store.document_index.contains_document_key(&doc_key.as_bytes().iter().cloned().collect())
    }

    pub fn read_stored_field(&self, field_ref: FieldRef, doc_ref: DocRef) -> Option<FieldValue> {
        let field_info = match self.schema().get(&field_ref) {
            Some(field_info) => field_info,
            None => return None,  // TODO Error?
        };

        let mut kb = KeyBuilder::stored_field_value(doc_ref.chunk(), doc_ref.ord(), field_ref.ord());

        match self.snapshot.get(&kb.key()) {
            Ok(Some(value)) => {
                match field_info.field_type {
                    FieldType::Text | FieldType::PlainString => {
                        Some(FieldValue::String(str::from_utf8(&value).unwrap().to_string()))
                    }
                    FieldType::I64 => {
                        Some(FieldValue::Integer(BigEndian::read_i64(&value)))
                    }
                    FieldType::Boolean => {
                        match value[..] {
                            [b't'] => Some(FieldValue::Boolean(true)),
                            [b'f'] => Some(FieldValue::Boolean(false)),
                            _ => None  // TODO Error
                        }
                    }
                    FieldType::DateTime => {
                        let timestamp_with_micros = BigEndian::read_i64(&value);
                        let timestamp = timestamp_with_micros / 1000000;
                        let micros = timestamp_with_micros % 1000000;
                        let nanos = micros * 1000;
                        let datetime = NaiveDateTime::from_timestamp(timestamp, nanos as u32);
                        Some(FieldValue::DateTime(DateTime::from_utc(datetime, UTC)))
                    }
                }
            }
            Ok(None) => None,
            Err(e) => None,  // TODO Error
        }
    }
}


#[cfg(test)]
mod tests {
    use std::fs::remove_dir_all;

    use rocksdb::{DB, Options, IteratorMode};
    use kite::{Term, Token, Document};
    use kite::document::FieldValue;
    use kite::schema::{Schema, FieldType, FIELD_INDEXED, FIELD_STORED, FieldRef};
    use kite::query::Query;
    use kite::query::term_scorer::TermScorer;
    use kite::collectors::top_score::TopScoreCollector;

    use super::RocksDBIndexStore;

    #[test]
    fn test_create() {
        remove_dir_all("test_indices/test_create");

        let store = RocksDBIndexStore::create("test_indices/test_create");
        assert!(store.is_ok());
    }

    #[test]
    fn test_open() {
        remove_dir_all("test_indices/test_open");

        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_err());

        // Create DB
        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, "test_indices/test_open").unwrap();

        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_ok());
    }

    fn make_test_store(path: &str) -> RocksDBIndexStore {
        let mut store = RocksDBIndexStore::create(path).unwrap();
        let mut title_field = store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        let mut body_field = store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        let mut pk_field = store.add_field("pk".to_string(), FieldType::I64, FIELD_STORED).unwrap();

        store.insert_or_update_document(Document {
            key: "test_doc".to_string(),
            indexed_fields: hashmap! {
                "title".to_string() => vec![
                    Token { term: Term::String("hello".to_string()), position: 1 },
                    Token { term: Term::String("world".to_string()), position: 2 },
                ],
                "body".to_string() => vec![
                    Token { term: Term::String("lorem".to_string()), position: 1 },
                    Token { term: Term::String("ipsum".to_string()), position: 2 },
                    Token { term: Term::String("dolar".to_string()), position: 3 },
                ],
            },
            stored_fields: hashmap! {
                "pk".to_string() => FieldValue::Integer(1),
            }
        });

        store.insert_or_update_document(Document {
            key: "another_test_doc".to_string(),
            indexed_fields: hashmap! {
                "title".to_string() => vec![
                    Token { term: Term::String("howdy".to_string()), position: 1 },
                    Token { term: Term::String("partner".to_string()), position: 2 },
                ],
                "body".to_string() => vec![
                    Token { term: Term::String("lorem".to_string()), position: 1 },
                    Token { term: Term::String("ipsum".to_string()), position: 2 },
                    Token { term: Term::String("dolar".to_string()), position: 3 },
                ],
            },
            stored_fields: hashmap! {
                "pk".to_string() => FieldValue::Integer(2),
            }
        });

        store.merge_chunks(vec![1, 2]);

        store
    }

    pub fn print_keys(db: &DB) {
        fn bytes_to_string(bytes: &Box<[u8]>) -> String {
            use std::char;

            let mut string = String::new();

            for byte in bytes.iter() {
                if *byte < 128 {
                    // ASCII character
                    string.push(char::from_u32(*byte as u32).unwrap());
                } else {
                    string.push('?');
                }
            }

            string
        }

        for (key, value) in db.iterator(IteratorMode::Start) {
            println!("{} = {:?}", bytes_to_string(&key), value);
        }
    }

    #[test]
    fn test() {
        remove_dir_all("test_indices/test");

        make_test_store("test_indices/test");

        let store = RocksDBIndexStore::open("test_indices/test").unwrap();

        let index_reader = store.reader();

        print_keys(&store.db);


        let query = Query::Disjunction {
            queries: vec![
                Query::MatchTerm {
                    field: "title".to_string(),
                    term: Term::String("howdy".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: "title".to_string(),
                    term: Term::String("partner".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::MatchTerm {
                    field: "title".to_string(),
                    term: Term::String("hello".to_string()),
                    scorer: TermScorer::default_with_boost(2.0f64),
                }
            ]
        };

        let mut collector = TopScoreCollector::new(10);
        index_reader.search(&mut collector, &query);

        let docs = collector.iter().collect::<Vec<_>>();
        println!("{:?}", docs);
    }
}
