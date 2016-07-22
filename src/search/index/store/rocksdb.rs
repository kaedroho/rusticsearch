use roaring::{RoaringBitmap, Iter};

use std::collections::{BTreeMap, HashMap};
use std::collections::btree_map::Keys;

use search::term::Term;
use search::document::Document;
use search::index::store::IndexStore;
use search::index::reader::{IndexReader, DocRefIterator};


#[derive(Debug)]
pub struct RocksDBIndexStore {
    docs: BTreeMap<u64, Document>,
    index: BTreeMap<Term, BTreeMap<String, RoaringBitmap<u64>>>,
    next_doc_id: u64,
    doc_key2id_map: HashMap<String, u64>,
}


impl RocksDBIndexStore {
    pub fn new() -> RocksDBIndexStore {
        RocksDBIndexStore {
            docs: BTreeMap::new(),
            index: BTreeMap::new(),
            next_doc_id: 1,
            doc_key2id_map: HashMap::new(),
        }
    }
}


impl<'a> IndexStore<'a> for RocksDBIndexStore {
    type Reader = RocksDBIndexStoreReader<'a>;

    fn reader(&'a self) -> RocksDBIndexStoreReader<'a> {
        RocksDBIndexStoreReader {
            store: self,
        }
    }

    fn insert_or_update_document(&mut self, doc: Document) {
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;

        // Put field contents in inverted index
        for (field_name, tokens) in doc.fields.iter() {
            let mut position: u32 = 1;

            for token in tokens.iter() {
                if !self.index.contains_key(&token.term) {
                    self.index.insert(token.term.clone(), BTreeMap::new());
                }

                let mut index_fields = self.index.get_mut(&token.term).unwrap();
                if !index_fields.contains_key(field_name) {
                    index_fields.insert(field_name.clone(), RoaringBitmap::new());
                }

                let mut index_docs = index_fields.get_mut(field_name).unwrap();
                index_docs.insert(doc_id);
            }
        }

        self.doc_key2id_map.insert(doc.key.clone(), doc_id);
        self.docs.insert(doc_id, doc);
    }

    fn remove_document_by_key(&mut self, doc_key: &str) -> bool {
        match self.doc_key2id_map.remove(doc_key) {
            Some(doc_id) => {
                self.docs.remove(&doc_id);

                true
            }
            None => false
        }
    }
}


pub struct RocksDBIndexStoreReader<'a> {
    store: &'a RocksDBIndexStore,
}


impl<'a> IndexReader<'a> for RocksDBIndexStoreReader<'a> {
    type AllDocRefIterator = RocksDBIndexStoreAllDocRefIterator<'a>;
    type TermDocRefIterator = RocksDBIndexStoreTermDocRefIterator<'a>;

    fn get_document_by_key(&self, doc_key: &str) -> Option<&Document> {
        match self.store.doc_key2id_map.get(doc_key) {
            Some(doc_id) => self.store.docs.get(doc_id),
            None => None,
        }
    }

    fn get_document_by_id(&self, doc_id: &u64) -> Option<&Document> {
        self.store.docs.get(doc_id)
    }

    fn contains_document_key(&self, doc_key: &str) -> bool {
        self.store.doc_key2id_map.contains_key(doc_key)
    }

    fn next_doc(&self, term: &Term, field_name: &str, previous_doc: Option<u64>) -> Option<u64> {
        let fields = match self.store.index.get(term) {
            Some(fields) => fields,
            None => return None,
        };

        let docs = match fields.get(field_name) {
            Some(docs) => docs,
            None => return None,
        };

        match previous_doc {
            Some(previous_doc) => {
                // Find first doc after specified doc
                // TODO: Speed this up (see section 2.1.2 of the IR book)
                for doc_id in docs.iter() {
                    if doc_id > previous_doc {
                        return Some(doc_id);
                    }
                }

                // Ran out of docs
                return None;
            }
            None => {
                // Previous doc not specified, return first doc
                match docs.iter().next() {
                    Some(doc_id) => Some(doc_id),
                    None => None,
                }
            }
        }
    }

    fn num_docs(&self) -> usize {
        self.store.docs.len()
    }

    fn iter_docids_all(&'a self) -> RocksDBIndexStoreAllDocRefIterator<'a> {
        RocksDBIndexStoreAllDocRefIterator {
            keys: self.store.docs.keys(),
        }
    }

    fn iter_docids_with_term(&'a self, term: &Term, field_name: &str) -> Option<RocksDBIndexStoreTermDocRefIterator<'a>> {
        let fields = match self.store.index.get(term) {
            Some(fields) => fields,
            None => return None,
        };

        let docs = match fields.get(field_name) {
            Some(docs) => docs,
            None => return None,
        };

        Some(RocksDBIndexStoreTermDocRefIterator {
            iterator: docs.iter(),
        })
    }

    fn iter_terms(&'a self) -> Box<Iterator<Item=&'a Term> + 'a> {
        Box::new(self.store.index.keys())
    }
}


pub struct RocksDBIndexStoreAllDocRefIterator<'a> {
    keys: Keys<'a, u64, Document>,
}

impl<'a> Iterator for RocksDBIndexStoreAllDocRefIterator<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.keys.next().cloned()
    }
}

impl<'a> DocRefIterator<'a> for RocksDBIndexStoreAllDocRefIterator<'a> {

}


pub struct RocksDBIndexStoreTermDocRefIterator<'a> {
    iterator: Iter<'a, u64>,
}

impl<'a> Iterator for RocksDBIndexStoreTermDocRefIterator<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.iterator.next()
    }
}

impl<'a> DocRefIterator<'a> for RocksDBIndexStoreTermDocRefIterator<'a> {

}


#[cfg(test)]
mod tests {
    use super::{RocksDBIndexStore, RocksDBIndexStoreReader};

    use search::term::Term;
    use search::analysis::Analyzer;
    use search::document::Document;
    use search::index::store::IndexStore;
    use search::index::reader::IndexReader;

    fn make_test_store() -> RocksDBIndexStore {
        let mut store = RocksDBIndexStore::new();

        store.insert_or_update_document(Document {
            key: "test_doc".to_string(),
            fields: btreemap! {
                "title".to_string() => Analyzer::Standard.run("hello world".to_string()),
                "body".to_string() => Analyzer::Standard.run("lorem ipsum dolar".to_string()),
            }
        });

        store.insert_or_update_document(Document {
            key: "test_doc".to_string(),
            fields: btreemap! {
                "title".to_string() => Analyzer::Standard.run("howdy partner".to_string()),
                "body".to_string() => Analyzer::Standard.run("lorem ipsum dolar".to_string()),
            }
        });

        store
    }

    #[test]
    fn test_num_docs() {
        let store = make_test_store();
        let reader = store.reader();

        assert_eq!(reader.num_docs(), 2);
    }

    #[test]
    fn test_all_docs_iterator() {
        let store = make_test_store();
        let reader = store.reader();

        assert_eq!(reader.iter_docids_all().count(), 2);
    }

    #[test]
    fn test_term_docs_iterator() {
        let store = make_test_store();
        let reader = store.reader();

        assert_eq!(reader.iter_docids_with_term(&Term::String("hello".to_string()), "title").unwrap().count(), 1);
    }
}
