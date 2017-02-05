extern crate kite;
extern crate rocksdb;
extern crate rustc_serialize;
extern crate byteorder;
extern crate chrono;
#[cfg(test)]
#[macro_use]
extern crate maplit;

mod key_builder;
mod segment;
mod segment_manager;
mod segment_ops;
mod segment_stats;
mod segment_builder;
mod term_dictionary;
mod document_index;
mod search;

use std::str;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;

use rocksdb::{DB, WriteBatch, Options, MergeOperands, Snapshot};
use kite::{Document, DocRef, TermRef};
use kite::document::FieldValue;
use kite::schema::{Schema, FieldType, FieldFlags, FieldRef, AddFieldError};
use rustc_serialize::json;
use byteorder::{ByteOrder, BigEndian};
use chrono::{NaiveDateTime, DateTime, UTC};

use key_builder::KeyBuilder;
use segment_manager::SegmentManager;
use term_dictionary::TermDictionaryManager;
use document_index::DocumentIndexManager;


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


#[derive(Debug)]
pub enum DocumentInsertError {
    /// A RocksDB error occurred
    RocksDBError(rocksdb::Error),

    /// The segment is full
    SegmentFull,
}


impl From<rocksdb::Error> for DocumentInsertError {
    fn from(e: rocksdb::Error) -> DocumentInsertError {
        DocumentInsertError::RocksDBError(e)
    }
}


impl From<segment_builder::DocumentInsertError> for DocumentInsertError {
    fn from(e: segment_builder::DocumentInsertError) -> DocumentInsertError {
        match e {
            segment_builder::DocumentInsertError::SegmentFull => DocumentInsertError::SegmentFull,
        }
    }
}


pub struct RocksDBIndexStore {
    schema: Arc<Schema>,
    db: DB,
    term_dictionary: TermDictionaryManager,
    segments: SegmentManager,
    document_index: DocumentIndexManager,
}


impl RocksDBIndexStore {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        opts.set_merge_operator("merge operator", merge_keys);
        opts.create_if_missing(true);
        let db = try!(DB::open(&opts, path));

        // Schema
        let schema = Schema::new();
        let schema_encoded = match json::encode(&schema) {
            Ok(schema_encoded) => schema_encoded,
            Err(e) => return Err(format!("schema encode error: {:?}", e).into()),
        };
        try!(db.put(b".schema", schema_encoded.as_bytes()));

        // Segment manager
        let segments = try!(SegmentManager::new(&db));

        // Term dictionary manager
        let term_dictionary = try!(TermDictionaryManager::new(&db));

        // Document index
        let document_index = try!(DocumentIndexManager::new(&db));

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            term_dictionary: term_dictionary,
            segments: segments,
            document_index: document_index,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<RocksDBIndexStore, String> {
        let mut opts = Options::default();
        opts.set_merge_operator("merge operator", merge_keys);
        let db = try!(DB::open(&opts, path));

        let schema = match try!(db.get(b".schema")) {
            Some(schema) => {
                let schema = schema.to_utf8().unwrap().to_string();
                match json::decode(&schema) {
                    Ok(schema) => schema,
                    Err(e) => return Err(format!("schema parse error: {:?}", e).into()),
                }
            }
            None => return Err("unable to find schema in store".into()),
        };

        // Segment manager
        let segments = try!(SegmentManager::open(&db));

        // Term dictionary manager
        let term_dictionary = try!(TermDictionaryManager::open(&db));

        // Document index
        let document_index = try!(DocumentIndexManager::open(&db));

        Ok(RocksDBIndexStore {
            schema: Arc::new(schema),
            db: db,
            term_dictionary: term_dictionary,
            segments: segments,
            document_index: document_index,
        })
    }

    pub fn path(&self) -> &Path {
        self.db.path()
    }

    pub fn add_field(&mut self, name: String, field_type: FieldType, field_flags: FieldFlags) -> Result<FieldRef, AddFieldError> {
        let mut schema_copy = (*self.schema).clone();
        let field_ref = try!(schema_copy.add_field(name, field_type, field_flags));
        self.schema = Arc::new(schema_copy);

        // FIXME: How do we throw this error?
        self.db.put(b".schema", json::encode(&self.schema).unwrap().as_bytes()).unwrap();

        Ok(field_ref)
    }

    pub fn remove_field(&mut self, field_ref: &FieldRef) -> bool {
        let mut schema_copy = (*self.schema).clone();
        let field_removed = schema_copy.remove_field(field_ref);

        if field_removed {
            self.schema = Arc::new(schema_copy);

            // FIXME: How do we throw this error?
            self.db.put(b".schema", json::encode(&self.schema).unwrap().as_bytes()).unwrap();
        }

        field_removed
    }

    pub fn insert_or_update_document(&self, doc: &Document) -> Result<(), DocumentInsertError> {
        // Build segment in memory
        let mut builder = segment_builder::SegmentBuilder::new();
        let doc_key = doc.key.clone();
        try!(builder.add_document(doc));

        // Write the segment
        let segment = try!(self.write_segment(&builder));

        // Update document index
        let doc_ref = DocRef::from_segment_ord(segment, 0);
        try!(self.document_index.insert_or_replace_key(&self.db, &doc_key.as_bytes().iter().cloned().collect(), doc_ref));

        Ok(())
    }

    pub fn write_segment(&self, builder: &segment_builder::SegmentBuilder) -> Result<u32, rocksdb::Error> {
        // Allocate a segment ID
        let segment = try!(self.segments.new_segment(&self.db));

        // Start write batch
        let mut write_batch = WriteBatch::default();

        // Set segment active flag, this will activate the segment as soon as the
        // write batch is written
        let kb = KeyBuilder::segment_active(segment);
        try!(write_batch.put(&kb.key(), b""));

        // Merge the term dictionary
        // Writes new terms to disk and generates mapping between the builder's term dictionary and the real one
        let mut term_dictionary_map: HashMap<TermRef, TermRef> = HashMap::new();
        for (term, current_term_ref) in builder.term_dictionary.iter() {
            let new_term_ref = try!(self.term_dictionary.get_or_create(&self.db, term));
            term_dictionary_map.insert(*current_term_ref, new_term_ref);
        }

        // Write term directories
        for (&(field_ref, term_ref), doc_ids) in builder.term_directories.iter() {
            let new_term_ref = term_dictionary_map.get(&term_ref).expect("TermRef not in term_dictionary_map");

            // Convert doc_id list to bytes
            let mut doc_ids_bytes = Vec::with_capacity(doc_ids.len() * 2);
            for doc_id in doc_ids.iter() {
                let mut doc_id_bytes = [0; 2];
                BigEndian::write_u16(&mut doc_id_bytes, *doc_id);
                doc_ids_bytes.push(doc_id_bytes[0]);
                doc_ids_bytes.push(doc_id_bytes[1]);
            }

            let kb = KeyBuilder::segment_dir_list(segment, field_ref.ord(), new_term_ref.ord());
            try!(write_batch.put(&kb.key(), &doc_ids_bytes));
        }

        // Write stored fields
        for (&(field_ref, doc_id, ref value_type), value) in builder.stored_field_values.iter() {
            let kb = KeyBuilder::stored_field_value(segment, doc_id, field_ref.ord(), value_type);
            try!(write_batch.put(&kb.key(), value));
        }

        // Write statistics
        for (name, value) in builder.statistics.iter() {
            let kb = KeyBuilder::segment_stat(segment, name);

            let mut value_bytes = [0; 8];
            BigEndian::write_i64(&mut value_bytes, *value);
            try!(write_batch.put(&kb.key(), &value_bytes));
        }

        // Write data
        try!(self.db.write(write_batch));

        Ok(segment)
    }

    pub fn remove_document_by_key(&self, doc_key: &str) -> Result<bool, rocksdb::Error> {
        match try!(self.document_index.delete_document_by_key(&self.db, &doc_key.as_bytes().iter().cloned().collect())) {
            Some(_doc_ref) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn reader<'a>(&'a self) -> RocksDBIndexReader<'a> {
        RocksDBIndexReader {
            store: &self,
            snapshot: self.db.snapshot(),
        }
    }
}


impl fmt::Debug for RocksDBIndexStore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksDBIndexStore {{ path: {:?} }}", self.db.path())
    }
}


pub enum StoredFieldReadError {
    /// The provided FieldRef wasn't valid for this index
    InvalidFieldRef(FieldRef),

    /// A RocksDB error occurred while reading from the disk
    RocksDBError(rocksdb::Error),

    /// A UTF-8 decode error occured while reading a Text field
    TextFieldUTF8DecodeError(Vec<u8>, str::Utf8Error),

    /// A boolean field was read but the value wasn't a boolean
    BooleanFieldDecodeError(Vec<u8>),

    /// An integer/datetime field was read but the value wasn't 8 bytes
    IntegerFieldValueSizeError(usize),
}


impl From<rocksdb::Error> for StoredFieldReadError {
    fn from(e: rocksdb::Error) -> StoredFieldReadError {
        StoredFieldReadError::RocksDBError(e)
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

    pub fn read_stored_field(&self, field_ref: FieldRef, doc_ref: DocRef) -> Result<Option<FieldValue>, StoredFieldReadError> {
        let field_info = match self.schema().get(&field_ref) {
            Some(field_info) => field_info,
            None => return Err(StoredFieldReadError::InvalidFieldRef(field_ref)),
        };

        let kb = KeyBuilder::stored_field_value(doc_ref.segment(), doc_ref.ord(), field_ref.ord(), b"val");

        match try!(self.snapshot.get(&kb.key())) {
            Some(value) => {
                match field_info.field_type {
                    FieldType::Text | FieldType::PlainString => {
                        match str::from_utf8(&value) {
                            Ok(value_str) => {
                                Ok(Some(FieldValue::String(value_str.to_string())))
                            }
                            Err(e) => {
                                Err(StoredFieldReadError::TextFieldUTF8DecodeError(value.to_vec(), e))
                            }
                        }
                    }
                    FieldType::I64 => {
                        if value.len() != 8 {
                            return Err(StoredFieldReadError::IntegerFieldValueSizeError(value.len()));
                        }

                        Ok(Some(FieldValue::Integer(BigEndian::read_i64(&value))))
                    }
                    FieldType::Boolean => {
                        if value[..] == [b't'] {
                            Ok(Some(FieldValue::Boolean(true)))
                        } else if value[..] == [b'f'] {
                            Ok(Some(FieldValue::Boolean(false)))
                        } else {
                            Err(StoredFieldReadError::BooleanFieldDecodeError(value.to_vec()))
                        }
                    }
                    FieldType::DateTime => {
                        if value.len() != 8 {
                            return Err(StoredFieldReadError::IntegerFieldValueSizeError(value.len()))
                        }

                        let timestamp_with_micros = BigEndian::read_i64(&value);
                        let timestamp = timestamp_with_micros / 1000000;
                        let micros = timestamp_with_micros % 1000000;
                        let nanos = micros * 1000;
                        let datetime = NaiveDateTime::from_timestamp(timestamp, nanos as u32);
                        Ok(Some(FieldValue::DateTime(DateTime::from_utc(datetime, UTC))))
                    }
                }
            }
            None => Ok(None),
        }
    }
}


#[cfg(test)]
mod tests {
    use std::fs::remove_dir_all;

    use rocksdb::{DB, Options};
    use kite::{Term, Token, Document};
    use kite::document::FieldValue;
    use kite::schema::{FieldType, FIELD_INDEXED, FIELD_STORED};
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

        // Check that it fails to open a DB which doesn't exist
        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_err());

        // Create the DB
        RocksDBIndexStore::create("test_indices/test_open");

        // Now try and open it
        let store = RocksDBIndexStore::open("test_indices/test_open");
        assert!(store.is_ok());
    }

    fn make_test_store(path: &str) -> RocksDBIndexStore {
        let mut store = RocksDBIndexStore::create(path).unwrap();
        let title_field = store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        let body_field = store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        let pk_field = store.add_field("pk".to_string(), FieldType::I64, FIELD_STORED).unwrap();

        store.insert_or_update_document(&Document {
            key: "test_doc".to_string(),
            indexed_fields: hashmap! {
                title_field => vec![
                    Token { term: Term::from_string("hello"), position: 1 },
                    Token { term: Term::from_string("world"), position: 2 },
                ],
                body_field => vec![
                    Token { term: Term::from_string("lorem"), position: 1 },
                    Token { term: Term::from_string("ipsum"), position: 2 },
                    Token { term: Term::from_string("dolar"), position: 3 },
                ],
            },
            stored_fields: hashmap! {
                pk_field => FieldValue::Integer(1),
            }
        }).unwrap();

        store.insert_or_update_document(&Document {
            key: "another_test_doc".to_string(),
            indexed_fields: hashmap! {
                title_field => vec![
                    Token { term: Term::from_string("howdy"), position: 1 },
                    Token { term: Term::from_string("partner"), position: 2 },
                ],
                body_field => vec![
                    Token { term: Term::from_string("lorem"), position: 1 },
                    Token { term: Term::from_string("ipsum"), position: 2 },
                    Token { term: Term::from_string("dolar"), position: 3 },
                ],
            },
            stored_fields: hashmap! {
                pk_field => FieldValue::Integer(2),
            }
        }).unwrap();

        store.merge_segments(&vec![1, 2]).unwrap();
        store.purge_segments(&vec![1, 2]).unwrap();

        store
    }

    pub fn print_keys(db: &DB) {
        fn bytes_to_string(bytes: &[u8]) -> String {
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

        let mut iter = db.iterator();
        while iter.next() {
            println!("{} = {:?}", bytes_to_string(&iter.key().unwrap()), iter.value().unwrap());
        }
    }

    #[test]
    fn test() {
        remove_dir_all("test_indices/test");

        make_test_store("test_indices/test");

        let store = RocksDBIndexStore::open("test_indices/test").unwrap();
        let title_field = store.schema.get_field_by_name("title").unwrap();

        let index_reader = store.reader();

        print_keys(&store.db);


        let query = Query::Disjunction {
            queries: vec![
                Query::Term {
                    field: title_field,
                    term: Term::from_string("howdy"),
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::Term {
                    field: title_field,
                    term: Term::from_string("partner"),
                    scorer: TermScorer::default_with_boost(2.0f64),
                },
                Query::Term {
                    field: title_field,
                    term: Term::from_string("hello"),
                    scorer: TermScorer::default_with_boost(2.0f64),
                }
            ]
        };

        let mut collector = TopScoreCollector::new(10);
        index_reader.search(&mut collector, &query).unwrap();

        let docs = collector.into_sorted_vec();
        println!("{:?}", docs);
    }
}
