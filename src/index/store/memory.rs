use std::collections::{BTreeMap, HashMap};

use term::Term;
use document::Document;


#[derive(Debug)]
pub struct MemoryIndexStore {
    pub docs: BTreeMap<u64, Document>,
    pub index: BTreeMap<Term, BTreeMap<String, Vec<(u64, u32)>>>,
    next_doc_num: u64,
    doc_id_map: HashMap<String, u64>,
}


impl MemoryIndexStore {
    pub fn new() -> MemoryIndexStore {
        MemoryIndexStore {
            docs: BTreeMap::new(),
            index: BTreeMap::new(),
            next_doc_num: 1,
            doc_id_map: HashMap::new(),
        }
    }

    pub fn get_document_by_id(&self, id: &str) -> Option<&Document> {
        match self.doc_id_map.get(id) {
            Some(doc_num) => self.docs.get(doc_num),
            None => None,
        }
    }

    pub fn contains_document_id(&self, id: &str) -> bool {
        self.doc_id_map.contains_key(id)
    }

    pub fn remove_document_by_id(&mut self, id: &str) -> bool {
        match self.doc_id_map.remove(id) {
            Some(doc_num) => {
                self.docs.remove(&doc_num);

                true
            }
            None => false
        }
    }

    pub fn insert_or_update_document(&mut self, doc: Document) {
        let doc_num = self.next_doc_num;
        self.next_doc_num += 1;

        // Put field contents in inverted index
        for (field_name, tokens) in doc.fields.iter() {
            let mut position: u32 = 1;

            for token in tokens.iter() {
                if !self.index.contains_key(&token.term) {
                    self.index.insert(token.term.clone(), BTreeMap::new());
                }

                let mut index_fields = self.index.get_mut(&token.term).unwrap();

                if !index_fields.contains_key(field_name) {
                    index_fields.insert(field_name.clone(), Vec::new());
                }

                let mut postings_list = index_fields.get_mut(field_name).unwrap();
                postings_list.push((doc_num, token.position));
            }
        }

        self.doc_id_map.insert(doc.id.clone(), doc_num);
        self.docs.insert(doc_num, doc);
    }

    pub fn next_phrase(&self, terms: &Vec<Term>, field_name: &str, position: Option<(u64, u32)>) -> Option<(u64, u32)> {
        let mut v = position;

        for term in terms.iter() {
            v = self.next(term, field_name, v);

            if v == None {
                return None;
            }
        }

        let mut u = v;
        for term in terms.iter().rev().skip(1) {
            u = self.prev(term, field_name, u);

            if u == None {
                return None;
            }
        }

        let v = v.unwrap();
        let u = u.unwrap();
        if v.0 == u.0 && v.1 - u.1 == terms.len() as u32 - 1 {
            Some((v.0, u.1))
        } else {
            self.next_phrase(terms, field_name, Some(u))
        }
    }

    pub fn next(&self, term: &Term, field_name: &str, position: Option<(u64, u32)>) -> Option<(u64, u32)> {
        let posting_fields = match self.index.get(term) {
            Some(posting_fields) => posting_fields,
            None => return None,
        };

        let postings = match posting_fields.get(field_name) {
            Some(postings) => postings,
            None => return None,
        };

        match position {
            Some(position) => {
                // Find first posting after specified position
                // TODO: Speed this up (see section 2.1.2 of the IR book)
                for posting in postings.iter() {
                    if *posting > position {
                        return Some(posting.clone());
                    }
                }

                // Ran out of postings
                return None;
            }
            None => {
                // Position not specified, return first posting
                return postings.first().cloned();
            }
        }
    }

    pub fn prev(&self, term: &Term, field_name: &str, position: Option<(u64, u32)>) -> Option<(u64, u32)> {
        let posting_fields = match self.index.get(term) {
            Some(posting_fields) => posting_fields,
            None => return None,
        };

        let postings = match posting_fields.get(field_name) {
            Some(postings) => postings,
            None => return None,
        };

        match position {
            Some(position) => {
                // Find first posting before specified position
                // TODO: Speed this up (see section 2.1.2 of the IR book)
                for posting in postings.iter().rev() {
                    if *posting < position {
                        return Some(posting.clone());
                    }
                }

                // Ran out of postings
                return None;
            }
            None => {
                // Position not specified, return last posting
                return postings.last().cloned();
            }
        }
    }
}
