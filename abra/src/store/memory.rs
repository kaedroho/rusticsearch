use roaring::{RoaringBitmap, Iter};

use std::collections::{BTreeMap, HashMap};
use std::collections::btree_map::Keys;

use document::Document;
use schema::FieldRef;
use store::{IndexStore, IndexReader, DocRefIterator};


#[derive(Debug)]
pub struct MemoryIndexStoreFieldTerm {
    pub docs: RoaringBitmap<u64>,
}


impl MemoryIndexStoreFieldTerm {
    pub fn new() -> MemoryIndexStoreFieldTerm {
        MemoryIndexStoreFieldTerm {
            docs: RoaringBitmap::new(),
        }
    }
}


#[derive(Debug)]
pub struct MemoryIndexStoreField {
    pub docs: RoaringBitmap<u64>,
    pub terms: BTreeMap<Vec<u8>, MemoryIndexStoreFieldTerm>,
    pub num_tokens: u64,
}


impl MemoryIndexStoreField {
    pub fn new() -> MemoryIndexStoreField {
        MemoryIndexStoreField {
            docs: RoaringBitmap::new(),
            terms: BTreeMap::new(),
            num_tokens: 0,
        }
    }
}


#[derive(Debug)]
pub struct MemoryIndexStore {
    docs: BTreeMap<u64, Document>,
    fields: HashMap<FieldRef, MemoryIndexStoreField>,
    next_doc_id: u64,
    doc_key2id_map: HashMap<String, u64>,
}


impl MemoryIndexStore {
    pub fn new() -> MemoryIndexStore {
        MemoryIndexStore {
            docs: BTreeMap::new(),
            fields: HashMap::new(),
            next_doc_id: 1,
            doc_key2id_map: HashMap::new(),
        }
    }
}


impl<'a> IndexStore<'a> for MemoryIndexStore {
    type Reader = MemoryIndexStoreReader<'a>;

    fn reader(&'a self) -> MemoryIndexStoreReader<'a> {
        MemoryIndexStoreReader {
            store: self,
        }
    }

    fn insert_or_update_document(&mut self, doc: Document) {
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;

        // Put field contents in inverted index
        for (field_ref, tokens) in doc.fields.iter() {
            for token in tokens.iter() {
                if !self.fields.contains_key(field_ref) {
                    self.fields.insert(field_ref.clone(), MemoryIndexStoreField::new());
                }

                let mut field = self.fields.get_mut(field_ref).unwrap();
                field.docs.insert(doc_id);
                field.num_tokens += 1;

                let term_bytes = token.term.to_bytes();
                if !field.terms.contains_key(&term_bytes) {
                    // TODO: We shouldn't need to clone here
                    field.terms.insert(term_bytes.clone(), MemoryIndexStoreFieldTerm::new());
                }

                let mut term = field.terms.get_mut(&term_bytes).unwrap();
                term.docs.insert(doc_id);
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


pub struct MemoryIndexStoreReader<'a> {
    store: &'a MemoryIndexStore,
}


impl<'a> IndexReader<'a> for MemoryIndexStoreReader<'a> {
    type AllDocRefIterator = MemoryIndexStoreAllDocRefIterator<'a>;
    type TermDocRefIterator = MemoryIndexStoreTermDocRefIterator<'a>;

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

    fn num_docs(&self) -> usize {
        self.store.docs.len()
    }

    fn iter_docids_all(&'a self) -> MemoryIndexStoreAllDocRefIterator<'a> {
        MemoryIndexStoreAllDocRefIterator {
            keys: self.store.docs.keys(),
        }
    }

    fn iter_docids_with_term(&'a self, term: &[u8], field_ref: &FieldRef) -> Option<MemoryIndexStoreTermDocRefIterator<'a>> {
        let field = match self.store.fields.get(field_ref) {
            Some(field) => field,
            None => return None,
        };

        let term = match field.terms.get(term) {
            Some(term) => term,
            None => return None,
        };

        Some(MemoryIndexStoreTermDocRefIterator {
            iterator: term.docs.iter(),
        })
    }

    fn iter_terms(&'a self, field_ref: &FieldRef) -> Option<Box<Iterator<Item=&'a [u8]> + 'a>> {
        let field = match self.store.fields.get(field_ref) {
            Some(field) => field,
            None => return None,
        };

        Some(Box::new(field.terms.keys().map(|t| &t[..])))
    }

    fn term_doc_freq(&'a self, term: &[u8], field_ref: &FieldRef) -> u64 {
        let field = match self.store.fields.get(field_ref) {
            Some(field) => field,
            None => return 0,
        };

        let term = match field.terms.get(term) {
            Some(term) => term,
            None => return 0,
        };

        term.docs.len()
    }

    fn total_tokens(&'a self, field_ref: &FieldRef) -> u64 {
        let field = match self.store.fields.get(field_ref) {
            Some(field) => field,
            None => return 0,
        };

        field.num_tokens
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
    use super::{MemoryIndexStore, MemoryIndexStoreReader};

    use term::Term;
    use token::Token;
    use document::Document;
    use schema::{Schema, FieldType, FieldRef};
    use store::{IndexStore, IndexReader};

    fn make_test_store() -> (MemoryIndexStore, Schema) {
        let mut store = MemoryIndexStore::new();
        let mut schema = Schema::new();
        let mut title_field = schema.add_field("title".to_string(), FieldType::Text);
        let mut body_field = schema.add_field("body".to_string(), FieldType::Text);

        store.insert_or_update_document(Document {
            key: "test_doc".to_string(),
            fields: hashmap! {
                title_field => vec![
                    Token { term: Term::String("hello".to_string()), position: 1 },
                    Token { term: Term::String("world".to_string()), position: 2 },
                ],
                body_field => vec![
                    Token { term: Term::String("lorem".to_string()), position: 1 },
                    Token { term: Term::String("ipsum".to_string()), position: 2 },
                    Token { term: Term::String("dolar".to_string()), position: 3 },
                ],
            }
        });

        store.insert_or_update_document(Document {
            key: "test_doc".to_string(),
            fields: hashmap! {
                title_field => vec![
                    Token { term: Term::String("howdy".to_string()), position: 1 },
                    Token { term: Term::String("partner".to_string()), position: 2 },
                ],
                body_field => vec![
                    Token { term: Term::String("lorem".to_string()), position: 1 },
                    Token { term: Term::String("ipsum".to_string()), position: 2 },
                    Token { term: Term::String("dolar".to_string()), position: 3 },
                ],
            }
        });

        (store, schema)
    }

    #[test]
    fn test_num_docs() {
        let (store, _) = make_test_store();
        let reader = store.reader();

        assert_eq!(reader.num_docs(), 2);
    }

    #[test]
    fn test_all_docs_iterator() {
        let (store, _) = make_test_store();
        let reader = store.reader();

        assert_eq!(reader.iter_docids_all().count(), 2);
    }

    #[test]
    fn test_term_docs_iterator() {
        let (store, schema) = make_test_store();
        let reader = store.reader();
        let title_field = schema.get_field_by_name("title").unwrap();

        assert_eq!(reader.iter_docids_with_term(&Term::String("hello".to_string()).to_bytes(), &title_field).unwrap().count(), 1);
    }
}
