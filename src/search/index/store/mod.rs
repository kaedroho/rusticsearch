pub mod memory;

use search::term::Term;
use search::document::Document;
use search::index::reader::IndexReader;


pub trait IndexStore<'a>: IndexReader<'a> {
    fn get_document_by_key(&self, doc_key: &str) -> Option<&Document>;
    fn get_document_by_id(&self, doc_id: &u64) -> Option<&Document>;
    fn contains_document_key(&self, doc_key: &str) -> bool;
    fn remove_document_by_key(&mut self, doc_key: &str) -> bool;
    fn insert_or_update_document(&mut self, doc: Document);
    fn next_doc(&self, term: &Term, field_name: &str, previous_doc: Option<u64>) -> Option<u64>;
}
