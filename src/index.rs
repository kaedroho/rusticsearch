use std::collections::{BTreeMap, HashMap, HashSet};

use term::Term;
use mapping::{Mapping, FieldMapping};
use document::Document;


#[derive(Debug)]
pub struct Index {
    pub mappings: HashMap<String, Mapping>,
    pub docs: BTreeMap<u64, Document>,
    pub index: BTreeMap<Term, BTreeMap<String, Vec<(u64, u32)>>>,
    pub aliases: HashSet<String>,
    next_doc_num: u64,
    doc_id_map: HashMap<String, u64>,
}


impl Index {
    pub fn new() -> Index {
        Index {
            mappings: HashMap::new(),
            docs: BTreeMap::new(),
            index: BTreeMap::new(),
            aliases: HashSet::new(),
            next_doc_num: 1,
            doc_id_map: HashMap::new(),
        }
    }

    pub fn get_mapping_by_name(&self, name: &str) -> Option<&Mapping> {
        self.mappings.get(name)
    }

    pub fn get_field_mapping_by_name(&self, name: &str) -> Option<&FieldMapping> {
        for mapping in self.mappings.values() {
            if let Some(ref field_mapping) = mapping.fields.get(name) {
                return Some(field_mapping);
            }
        }

        None
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

    pub fn initialise(&mut self) {}
}
