use std::collections::{BTreeMap, HashMap};

use search::term::Term;
use search::document::Document;


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

    pub fn num_docs(&self) -> usize {
        self.docs.len()
    }

    pub fn iter_docs<'a>(&'a self) -> Box<Iterator<Item=&'a Document> + 'a> {
        Box::new(self.docs.values())
    }
}
