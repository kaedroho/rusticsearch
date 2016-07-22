pub mod memory;
pub mod rocksdb;

use search::document::Document;
use search::index::reader::IndexReader;


pub trait IndexStore<'a> {
    type Reader: IndexReader<'a>;

    fn reader(&'a self) -> Self::Reader;
    fn insert_or_update_document(&mut self, doc: Document);
    fn remove_document_by_key(&mut self, doc_key: &str) -> bool;
}
