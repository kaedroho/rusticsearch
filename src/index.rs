use std::collections::{BTreeMap, HashMap, HashSet};

use mapping::{Mapping, FieldMapping};
use document::Document;


#[derive(Debug)]
pub struct Index {
    pub mappings: HashMap<String, Mapping>,
    pub docs: BTreeMap<u64, Document>,
    pub aliases: HashSet<String>,
    next_doc_num: u64,
    doc_id_map: HashMap<String, u64>,
}


impl Index {
    pub fn new() -> Index {
        Index {
            mappings: HashMap::new(),
            docs: BTreeMap::new(),
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

        self.doc_id_map.insert(doc.id.clone(), doc_num);
        self.docs.insert(doc_num, doc);
    }

    pub fn initialise(&mut self) {}
}
