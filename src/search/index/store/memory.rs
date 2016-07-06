use std::collections::{BTreeMap, HashMap};
use std::collections::Bound::{Excluded, Unbounded};

use search::term::Term;
use search::document::Document;
use search::index::reader::{IndexReader, DocRefIterator};


#[derive(Debug)]
pub struct MemoryIndexStore {
    docs: BTreeMap<u64, Document>,
    index: BTreeMap<Term, BTreeMap<String, BTreeMap<u64, Vec<u32>>>>,
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
                    index_fields.insert(field_name.clone(), BTreeMap::new());
                }

                let mut index_docs = index_fields.get_mut(field_name).unwrap();
                if !index_docs.contains_key(&doc_id) {
                    index_docs.insert(doc_id, Vec::new());
                }

                let mut postings_list = index_docs.get_mut(&doc_id).unwrap();
                postings_list.push(token.position);
            }
        }

        self.doc_key2id_map.insert(doc.key.clone(), doc_id);
        self.docs.insert(doc_id, doc);
    }

    pub fn next_doc(&self, term: &Term, field_name: &str, previous_doc: Option<u64>) -> Option<(u64, usize)> {
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
                for (doc_id, postings) in docs.iter() {
                    if *doc_id > previous_doc {
                        return Some((*doc_id, postings.len()));
                    }
                }

                // Ran out of docs
                return None;
            }
            None => {
                // Previous doc not specified, return first doc
                match docs.iter().next() {
                    Some((doc_id, postings)) => Some((*doc_id, postings.len())),
                    None => None,
                }
            }
        }
    }

    pub fn next_doc_all(&self, position: Option<u64>) -> Option<u64> {
        match position {
            Some(doc_id) => {
                self.docs.range(Excluded(&doc_id), Unbounded).map(|(doc_id, doc)| *doc_id).nth(0)
            }
            None => {
                self.docs.keys().nth(0).cloned()
            }
        }
    }

    pub fn iter_terms<'a>(&'a self) -> Box<Iterator<Item=&'a Term> + 'a> {
        Box::new(self.index.keys())
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
            store: self,
            last_doc: None,
        }
    }

    fn iter_docids_with_term(&'a self, term: Term, field_name: String) -> MemoryIndexStoreTermDocRefIterator<'a> {
        MemoryIndexStoreTermDocRefIterator {
            store: self,
            term: term,
            field_name: field_name,
            last_doc: None,
        }
    }
}


struct MemoryIndexStoreAllDocRefIterator<'a> {
    store: &'a MemoryIndexStore,
    last_doc: Option<u64>,
}

impl<'a> Iterator for MemoryIndexStoreAllDocRefIterator<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.last_doc = self.store.next_doc_all(self.last_doc);

        self.last_doc
    }
}

impl<'a> DocRefIterator<'a> for MemoryIndexStoreAllDocRefIterator<'a> {

}


struct MemoryIndexStoreTermDocRefIterator<'a> {
    store: &'a MemoryIndexStore,
    term: Term,
    field_name: String,
    last_doc: Option<u64>,
}

impl<'a> Iterator for MemoryIndexStoreTermDocRefIterator<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.last_doc = match self.store.next_doc(&self.term, &self.field_name, self.last_doc) {
            Some((doc_id, term_freq)) => Some(doc_id),
            None => None,
        };

        self.last_doc
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

        assert_eq!(store.iter_docids_with_term(Term::String("hello".to_string()), "title".to_string()).count(), 1);
    }
}
