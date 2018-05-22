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

use rocksdb::{self, DB, WriteBatch, Options, MergeOperands, Snapshot};
use search::{Document, DocId, TermId};
use search::document::FieldValue;
use search::schema::{Schema, FieldType, FieldFlags, FieldId, AddFieldError};
use search::segment::SegmentId;
use byteorder::{ByteOrder, LittleEndian};
use chrono::{NaiveDateTime, DateTime, Utc};
use fnv::FnvHashMap;
use serde_json;

use self::key_builder::KeyBuilder;
use self::segment_manager::SegmentManager;
use self::term_dictionary::TermDictionaryManager;
use self::document_index::DocumentIndexManager;

fn merge_keys(key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Option<Vec<u8>> {
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

            Some(new_val)
        }
        b's' => {
            // Statistic
            // An i64 number that can be incremented or decremented
            let mut value = match existing_val {
                Some(existing_val) => LittleEndian::read_i64(existing_val),
                None => 0
            };

            for op in operands {
                value += LittleEndian::read_i64(op);
            }

            let mut buf = [0; 8];
            LittleEndian::write_i64(&mut buf, value);
            Some(buf.iter().cloned().collect())
        }
        _ => {
            // Unrecognised key, fallback to emulating a put operation (by taking the last value)
            Some(operands.last().unwrap().iter().cloned().collect())
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

pub struct RocksDBStore {
    schema: Arc<Schema>,
    db: DB,
    term_dictionary: TermDictionaryManager,
    segments: SegmentManager,
    document_index: DocumentIndexManager,
}

impl RocksDBStore {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<RocksDBStore, String> {
        let mut opts = Options::default();
        opts.set_merge_operator("merge operator", merge_keys, None);
        opts.create_if_missing(true);
        let db = try!(DB::open(&opts, path));

        // Schema
        let schema = Schema::new();
        let schema_encoded = match serde_json::to_string(&schema) {
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

        Ok(RocksDBStore {
            schema: Arc::new(schema),
            db: db,
            term_dictionary: term_dictionary,
            segments: segments,
            document_index: document_index,
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<RocksDBStore, String> {
        let mut opts = Options::default();
        opts.set_merge_operator("merge operator", merge_keys, None);
        let db = try!(DB::open(&opts, path));

        let schema = match try!(db.get(b".schema")) {
            Some(schema) => {
                let schema = schema.to_utf8().unwrap().to_string();
                match serde_json::from_str(&schema) {
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

        Ok(RocksDBStore {
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

    pub fn add_field(&mut self, name: String, field_type: FieldType, field_flags: FieldFlags) -> Result<FieldId, AddFieldError> {
        let mut schema_copy = (*self.schema).clone();
        let field_id = try!(schema_copy.add_field(name, field_type, field_flags));
        self.schema = Arc::new(schema_copy);

        // FIXME: How do we throw this error?
        self.db.put(b".schema", serde_json::to_string(&*self.schema).unwrap().as_bytes()).unwrap();

        Ok(field_id)
    }

    pub fn remove_field(&mut self, field_id: &FieldId) -> bool {
        let mut schema_copy = (*self.schema).clone();
        let field_removed = schema_copy.remove_field(field_id);

        if field_removed {
            self.schema = Arc::new(schema_copy);

            // FIXME: How do we throw this error?
            self.db.put(b".schema", serde_json::to_string(&*self.schema).unwrap().as_bytes()).unwrap();
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
        let doc_id = DocId(SegmentId(segment), 0);
        try!(self.document_index.insert_or_replace_key(&self.db, &doc_key.as_bytes().iter().cloned().collect(), doc_id));

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
        let mut term_dictionary_map: FnvHashMap<TermId, TermId> = FnvHashMap::default();
        for (term, current_term_id) in builder.term_dictionary.iter() {
            let new_term_id = try!(self.term_dictionary.get_or_create(&self.db, term));
            term_dictionary_map.insert(*current_term_id, new_term_id);
        }

        // Write term directories
        for (&(field_id, term_id), term_directory) in builder.term_directories.iter() {
            let new_term_id = term_dictionary_map.get(&term_id).expect("TermId not in term_dictionary_map");

            // Serialise
            let mut term_directory_bytes = Vec::new();
            term_directory.serialize_into(&mut term_directory_bytes).unwrap();

            // Write
            let kb = KeyBuilder::segment_dir_list(segment, field_id.0, new_term_id.0);
            try!(write_batch.put(&kb.key(), &term_directory_bytes));
        }

        // Write stored fields
        for (&(field_id, doc_id, ref value_type), value) in builder.stored_field_values.iter() {
            let kb = KeyBuilder::stored_field_value(segment, doc_id, field_id.0, value_type);
            try!(write_batch.put(&kb.key(), value));
        }

        // Write statistics
        for (name, value) in builder.statistics.iter() {
            let kb = KeyBuilder::segment_stat(segment, name);

            let mut value_bytes = [0; 8];
            LittleEndian::write_i64(&mut value_bytes, *value);
            try!(write_batch.put(&kb.key(), &value_bytes));
        }

        // Write data
        try!(self.db.write(write_batch));

        Ok(segment)
    }

    pub fn remove_document_by_key(&self, doc_key: &str) -> Result<bool, rocksdb::Error> {
        match try!(self.document_index.delete_document_by_key(&self.db, &doc_key.as_bytes().iter().cloned().collect())) {
            Some(_doc_id) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn reader<'a>(&'a self) -> RocksDBReader<'a> {
        RocksDBReader {
            store: &self,
            snapshot: self.db.snapshot(),
        }
    }
}

impl fmt::Debug for RocksDBStore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksDBStore {{ path: {:?} }}", self.db.path())
    }
}

pub enum StoredFieldReadError {
    /// The provided FieldId wasn't valid for this index
    InvalidFieldId(FieldId),

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

pub struct RocksDBReader<'a> {
    store: &'a RocksDBStore,
    snapshot: Snapshot<'a>
}

impl<'a> RocksDBReader<'a> {
    pub fn schema(&self) -> &Schema {
        &self.store.schema
    }

    pub fn contains_document_key(&self, doc_key: &str) -> bool {
        // TODO: use snapshot
        self.store.document_index.contains_document_key(&doc_key.as_bytes().iter().cloned().collect())
    }

    pub fn read_stored_field(&self, field_id: FieldId, doc_id: DocId) -> Result<Option<FieldValue>, StoredFieldReadError> {
        let field_info = match self.schema().get(&field_id) {
            Some(field_info) => field_info,
            None => return Err(StoredFieldReadError::InvalidFieldId(field_id)),
        };

        let kb = KeyBuilder::stored_field_value((doc_id.0).0, doc_id.1, field_id.0, b"val");

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

                        Ok(Some(FieldValue::Integer(LittleEndian::read_i64(&value))))
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

                        let timestamp_with_micros = LittleEndian::read_i64(&value);
                        let timestamp = timestamp_with_micros / 1000000;
                        let micros = timestamp_with_micros % 1000000;
                        let nanos = micros * 1000;
                        let datetime = NaiveDateTime::from_timestamp(timestamp, nanos as u32);
                        Ok(Some(FieldValue::DateTime(DateTime::from_utc(datetime, Utc))))
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
    use std::path::Path;

    use rocksdb::DB;
    use fnv::FnvHashMap;
    use search::{Term, Token, Document};
    use search::document::FieldValue;
    use search::schema::{FieldType, FIELD_INDEXED, FIELD_STORED};
    use search::query::Query;
    use search::query::term_scorer::TermScorer;
    use search::collectors::top_score::TopScoreCollector;

    use super::RocksDBStore;

    fn remove_dir_all_ignore_error<P: AsRef<Path>>(path: P) {
        match remove_dir_all(&path) {
            Ok(_) => {}
            Err(_) => {}  // Don't care if this fails
        }
    }

    #[test]
    fn test_create() {
        remove_dir_all_ignore_error("test_indices/test_create");

        let store = RocksDBStore::create("test_indices/test_create");
        assert!(store.is_ok());
    }

    #[test]
    fn test_open() {
        remove_dir_all_ignore_error("test_indices/test_open");

        // Check that it fails to open a DB which doesn't exist
        let store = RocksDBStore::open("test_indices/test_open");
        assert!(store.is_err());

        // Create the DB
        RocksDBStore::create("test_indices/test_open").expect("failed to create test DB");

        // Now try and open it
        let store = RocksDBStore::open("test_indices/test_open");
        assert!(store.is_ok());
    }

    fn make_test_store(path: &str) -> RocksDBStore {
        let mut store = RocksDBStore::create(path).unwrap();
        let title_field = store.add_field("title".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        let body_field = store.add_field("body".to_string(), FieldType::Text, FIELD_INDEXED).unwrap();
        let pk_field = store.add_field("pk".to_string(), FieldType::I64, FIELD_STORED).unwrap();


        let mut indexed_fields = FnvHashMap::default();
        indexed_fields.insert(
            title_field,
            vec![
                Token { term: Term::from_string("hello"), position: 1 },
                Token { term: Term::from_string("world"), position: 2 },
            ].into()
        );
        indexed_fields.insert(
            body_field,
            vec![
                Token { term: Term::from_string("lorem"), position: 1 },
                Token { term: Term::from_string("ipsum"), position: 2 },
                Token { term: Term::from_string("dolar"), position: 3 },
            ].into()
        );

        let mut stored_fields = FnvHashMap::default();
        stored_fields.insert(
            pk_field,
            FieldValue::Integer(1)
        );

        store.insert_or_update_document(&Document {
            key: "test_doc".to_string(),
            indexed_fields: indexed_fields,
            stored_fields: stored_fields,
        }).unwrap();

        let mut indexed_fields = FnvHashMap::default();
        indexed_fields.insert(
            title_field,
            vec![
                Token { term: Term::from_string("howdy"), position: 1 },
                Token { term: Term::from_string("partner"), position: 2 },
            ].into()
        );
        indexed_fields.insert(
            body_field,
            vec![
                Token { term: Term::from_string("lorem"), position: 1 },
                Token { term: Term::from_string("ipsum"), position: 2 },
                Token { term: Term::from_string("dolar"), position: 3 },
            ].into()
        );

        let mut stored_fields = FnvHashMap::default();
        stored_fields.insert(
            pk_field,
            FieldValue::Integer(2)
        );

        store.insert_or_update_document(&Document {
            key: "another_test_doc".to_string(),
            indexed_fields: indexed_fields,
            stored_fields: stored_fields,
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

        let mut iter = db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            println!("{} = {:?}", bytes_to_string(&iter.key().unwrap()), iter.value().unwrap());

            iter.next();
        }
    }

    #[test]
    fn test() {
        remove_dir_all_ignore_error("test_indices/test");

        make_test_store("test_indices/test");

        let store = RocksDBStore::open("test_indices/test").unwrap();
        let title_field = store.schema.get_field_by_name("title").unwrap();

        let index_reader = store.reader();

        print_keys(&store.db);


        let query = Query::Disjunction {
            queries: vec![
                Query::Term {
                    field: title_field,
                    term: Term::from_string("howdy"),
                    scorer: TermScorer::default_with_boost(2.0f32),
                },
                Query::Term {
                    field: title_field,
                    term: Term::from_string("partner"),
                    scorer: TermScorer::default_with_boost(2.0f32),
                },
                Query::Term {
                    field: title_field,
                    term: Term::from_string("hello"),
                    scorer: TermScorer::default_with_boost(2.0f32),
                }
            ]
        };

        let mut collector = TopScoreCollector::new(10);
        index_reader.search(&mut collector, &query).unwrap();

        let docs = collector.into_sorted_vec();
        println!("{:?}", docs);
    }
}
