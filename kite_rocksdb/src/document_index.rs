use std::sync::RwLock;
use std::collections::{BTreeMap, HashMap};

use rocksdb::{DB, Writable, WriteBatch, IteratorMode, Direction};
use byteorder::{ByteOrder, BigEndian, WriteBytesExt};

use errors::{RocksDBReadError, RocksDBWriteError};
use key_builder::KeyBuilder;
use segment_merge::SegmentMergeError;
use search::doc_id_set::DocIdSet;


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct DocRef(u32, u16);


impl DocRef {
    pub fn segment(&self) -> u32 {
        self.0
    }

    pub fn ord(&self) -> u16 {
        self.1
    }

    pub fn as_u64(&self) -> u64 {
        (self.0 as u64) << 16 | (self.1 as u64)
    }

    pub fn from_segment_ord(segment: u32, ord: u16) -> DocRef {
        DocRef(segment, ord)
    }

    pub fn from_u64(val: u64) -> DocRef {
        let segment = (val >> 16) & 0xFFFFFFFF;
        let ord = val & 0xFFFF;
        DocRef(segment as u32, ord as u16)
    }
}


/// Manages the index's "document index"
pub struct DocumentIndexManager {
    primary_key_index: RwLock<BTreeMap<Vec<u8>, DocRef>>,
}


impl DocumentIndexManager {
    /// Generates a new document index
    pub fn new(_db: &DB) -> Result<DocumentIndexManager, RocksDBWriteError> {
        Ok(DocumentIndexManager {
            primary_key_index: RwLock::new(BTreeMap::new()),
        })
    }

    /// Loads the document index from an index
    pub fn open(db: &DB) -> Result<DocumentIndexManager, RocksDBReadError> {
        // Read primary key index
        let mut primary_key_index = BTreeMap::new();
        for (k, v) in db.iterator(IteratorMode::From(b"k", Direction::Forward)) {
            if k[0] != b'k' {
                break;
            }

            let segment = BigEndian::read_u32(&v[0..4]);
            let ord = BigEndian::read_u16(&v[4..6]);
            let doc_ref = DocRef::from_segment_ord(segment, ord);

            primary_key_index.insert(k[1..].to_vec(), doc_ref);
        }

        Ok(DocumentIndexManager {
            primary_key_index: RwLock::new(primary_key_index),
        })
    }

    fn delete_document_by_ref_unchecked(&self, write_batch: &WriteBatch, doc_ref: DocRef) -> Result<(), RocksDBWriteError> {
        let kb = KeyBuilder::segment_del_list(doc_ref.segment());
        let mut previous_doc_id_bytes = [0; 2];
        BigEndian::write_u16(&mut previous_doc_id_bytes, doc_ref.ord());
        if let Err(e) = write_batch.merge(&kb.key(), &previous_doc_id_bytes) {
            return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e));
        }

        // Increment deleted docs
        let kb = KeyBuilder::segment_stat(doc_ref.segment(), b"deleted_docs");
        let mut inc_bytes = [0; 8];
        BigEndian::write_i64(&mut inc_bytes, 1);
        if let Err(e) = write_batch.merge(&kb.key(), &inc_bytes) {
            return Err(RocksDBWriteError::new_merge(kb.key().to_vec(), e));
        }

        Ok(())
    }

    pub fn insert_or_replace_key(&self, db: &DB, key: &Vec<u8>, doc_ref: DocRef) -> Result<Option<DocRef>, RocksDBWriteError> {
        // Update primary_key_index
        let write_batch = WriteBatch::default();
        let previous_doc_ref = self.primary_key_index.write().unwrap().insert(key.clone(), doc_ref);

        let kb = KeyBuilder::primary_key_index(key);
        let mut doc_ref_bytes = [0; 6];
        BigEndian::write_u32(&mut doc_ref_bytes, doc_ref.segment());
        BigEndian::write_u16(&mut doc_ref_bytes[4..], doc_ref.ord());
        if let Err(e) = write_batch.put(&kb.key(), &doc_ref_bytes) {
            return Err(RocksDBWriteError::new_put(kb.key().to_vec(), e));
        }

        // If there was a document there previously, delete it
        if let Some(previous_doc_ref) = previous_doc_ref {
            try!(self.delete_document_by_ref_unchecked(&write_batch, previous_doc_ref));
        }

        // Write document data
        if let Err(e) = db.write(write_batch) {
            return Err(RocksDBWriteError::new_commit_write_batch(e));
        }

        Ok(previous_doc_ref)
    }

    pub fn delete_document_by_key(&self, db: &DB, key: &Vec<u8>) -> Result<Option<DocRef>, RocksDBWriteError> {
        // Remove document from index
        let doc_ref = self.primary_key_index.write().unwrap().remove(key);

        if let Some(doc_ref) = doc_ref {
            let mut write_batch = WriteBatch::default();

            try!(self.delete_document_by_ref_unchecked(&mut write_batch, doc_ref));

            if let Err(e) = db.write(write_batch) {
                return Err(RocksDBWriteError::new_commit_write_batch(e));
            }
        }

        Ok(doc_ref)
    }

    pub fn contains_document_key(&self, key: &Vec<u8>) -> bool {
        self.primary_key_index.read().unwrap().contains_key(key)
    }

    pub fn commit_segment_merge(&self, db: &DB, write_batch: WriteBatch, source_segments: &Vec<u32>, dest_segment: u32, doc_ref_mapping: &HashMap<DocRef, u16>) -> Result<(), SegmentMergeError> {
        // Lock the primary key index
        let mut primary_key_index = self.primary_key_index.write().unwrap();

        // Update primary keys to point to their new locations
        let mut keys_to_update: HashMap<Vec<u8>, DocRef> = HashMap::with_capacity(doc_ref_mapping.len());
        for (key, doc_ref) in primary_key_index.iter() {
            if doc_ref_mapping.contains_key(&doc_ref) {
                keys_to_update.insert(key.clone(), *doc_ref);
            }
        }

        for (key, doc_ref) in keys_to_update {
            let new_doc_ord = doc_ref_mapping.get(&doc_ref).unwrap();
            let new_doc_ref = DocRef::from_segment_ord(dest_segment, *new_doc_ord);

            primary_key_index.insert(key, new_doc_ref);
        }

        // Merge deletion lists
        // Must be done while the primary_key_index is locked as this prevents any more documents being deleted
        let mut deletion_list = Vec::new();
        for source_segment in source_segments {
            let kb = KeyBuilder::segment_del_list(*source_segment);
            match db.get(&kb.key()) {
                Ok(Some(docid_set)) => {
                    for doc_id in DocIdSet::FromRDB(docid_set).iter() {
                        let doc_ref = DocRef::from_segment_ord(*source_segment, doc_id);
                        let new_doc_id = doc_ref_mapping.get(&doc_ref).unwrap();
                        deletion_list.write_u16::<BigEndian>(*new_doc_id).unwrap();
                    }
                }
                Ok(None) => {},
                Err(e) => return Err(RocksDBReadError::new(kb.key().to_vec(), e).into()),
            }
        }

        let kb = KeyBuilder::segment_del_list(dest_segment);
        if let Err(e) = db.put(&kb.key(), &deletion_list) {
            return Err(RocksDBWriteError::new_put(kb.key().to_vec(), e).into());
        }

        // Commit!
        if let Err(e) = db.write(write_batch) {
            return Err(RocksDBWriteError::new_commit_write_batch(e).into());
        }

        Ok(())
    }
}
