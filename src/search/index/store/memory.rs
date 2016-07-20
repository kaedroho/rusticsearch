use roaring::{RoaringBitmap, Iter};

use std::collections::{BTreeMap, HashMap};
use std::collections::btree_map::Keys;

use search::term::Term;
use search::document::Document;
use search::index::reader::{IndexReader, DocRefIterator};


#[derive(Debug)]
pub struct MemoryIndexStore {
    docs: BTreeMap<u64, Document>,
    index: BTreeMap<Term, BTreeMap<String, RoaringBitmap<u64>>>,
    next_doc_id: u64,
    doc_key2id_map: HashMap<String, u64>,
}


impl MemoryIndexStore {
    pub fn new() -> MemoryIndexStore {
        MemoryIndexStore {
            docs: BTreeMap::new(),
            index: BTreeMap::new(),
            next_doc_id: 1,
            doc_key2id_map: HashMap::new(),
        }
    }

    pub fn get_document_by_key(&self, doc_key: &str) -> Option<&Document> {
        match self.doc_key2id_map.get(doc_key) {
            Some(doc_id) => self.docs.get(doc_id),
            None => None,
        }
    }

    pub fn get_document_by_id(&self, doc_id: &u64) -> Option<&Document> {
        self.docs.get(doc_id)
    }

    pub fn contains_document_key(&self, doc_key: &str) -> bool {
        self.doc_key2id_map.contains_key(doc_key)
    }

    pub fn remove_document_by_key(&mut self, doc_key: &str) -> bool {
        match self.doc_key2id_map.remove(doc_key) {
            Some(doc_id) => {
                self.docs.remove(&doc_id);

                true
            }
            None => false
        }
    }

    pub fn insert_or_update_document(&mut self, doc: Document) {
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

    pub fn next_doc(&self, term: &Term, field_name: &str, previous_doc: Option<u64>) -> Option<u64> {
        let fields = match self.index.get(term) {
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
}


impl<'a> IndexReader<'a> for MemoryIndexStore {
    type AllDocRefIterator = MemoryIndexStoreAllDocRefIterator<'a>;
    type TermDocRefIterator = MemoryIndexStoreTermDocRefIterator<'a>;

    fn num_docs(&self) -> usize {
        self.docs.len()
    }

    fn iter_docids_all(&'a self) -> MemoryIndexStoreAllDocRefIterator<'a> {
        MemoryIndexStoreAllDocRefIterator {
            keys: self.docs.keys(),
        }
    }

    fn iter_docids_with_term(&'a self, term: &Term, field_name: &str) -> MemoryIndexStoreTermDocRefIterator<'a> {
        let fields = match self.index.get(term) {
            Some(fields) => fields,
            None => panic!("FOO"),
        };

        let docs = match fields.get(field_name) {
            Some(docs) => docs,
            None => panic!("FOO"),
        };

        MemoryIndexStoreTermDocRefIterator {
            iterator: docs.iter(),
        }
    }

    fn iter_terms(&'a self) -> Box<Iterator<Item=&'a Term> + 'a> {
        Box::new(self.index.keys())
    }
}


pub struct MemoryIndexStoreAllDocRefIterator<'a> {
    keys: Keys<'a, u64, Document>,
}

impl<'a> Iterator for MemoryIndexStoreAllDocRefIterator<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.keys.next().cloned()
    }
}

impl<'a> DocRefIterator<'a> for MemoryIndexStoreAllDocRefIterator<'a> {

}


pub struct MemoryIndexStoreTermDocRefIterator<'a> {
    iterator: Iter<'a, u64>,
}

impl<'a> Iterator for MemoryIndexStoreTermDocRefIterator<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.iterator.next()
    }
}

impl<'a> DocRefIterator<'a> for MemoryIndexStoreTermDocRefIterator<'a> {

}


#[cfg(test)]
mod tests {
    use super::MemoryIndexStore;

    use search::term::Term;
    use search::analysis::Analyzer;
    use search::document::Document;
    use search::index::reader::IndexReader;

    fn make_test_store() -> MemoryIndexStore {
        let mut store = MemoryIndexStore::new();

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

        assert_eq!(store.num_docs(), 2);
    }

    #[test]
    fn test_all_docs_iterator() {
        let store = make_test_store();

        assert_eq!(store.iter_docids_all().count(), 2);
    }

    #[test]
    fn test_term_docs_iterator() {
        let store = make_test_store();

        assert_eq!(store.iter_docids_with_term(&Term::String("hello".to_string()), "title").count(), 1);
    }
}
