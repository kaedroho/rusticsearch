pub mod memory;

use search::document::Document;
use search::index::reader::IndexReader;


pub trait IndexStore<'a>: IndexReader<'a> {
    fn remove_document_by_key(&mut self, doc_key: &str) -> bool;
    fn insert_or_update_document(&mut self, doc: Document);
}
