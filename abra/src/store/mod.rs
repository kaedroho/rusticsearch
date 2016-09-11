pub mod memory;

use document::Document;
use schema::{Schema, FieldType, FieldRef, AddFieldError};


pub trait IndexReader<'a> {
    type AllDocRefIterator: DocRefIterator<'a>;
    type TermDocRefIterator: DocRefIterator<'a>;

    fn schema(&self) -> &Schema;
    fn get_document_by_key(&self, doc_key: &str) -> Option<&Document>;
    fn get_document_by_id(&self, doc_id: &u64) -> Option<&Document>;
    fn contains_document_key(&self, doc_key: &str) -> bool;
    fn num_docs(&self) -> usize;
    fn iter_all_docs(&'a self) -> Self::AllDocRefIterator;
    fn iter_docs_with_term(&'a self, term: &[u8], field_ref: &FieldRef) -> Option<Self::TermDocRefIterator>;
    fn iter_all_terms(&'a self, field_ref: &FieldRef) -> Option<Box<Iterator<Item=&'a [u8]> + 'a>>;
    fn num_docs_with_term(&'a self, term: &[u8], field_ref: &FieldRef) -> u64;
    fn total_tokens(&'a self, field_ref: &FieldRef) -> u64;
    //pub fn retrieve_document(&self, &Self::DocRef) -> Document;
}


pub trait DocRefIterator<'a>: Iterator<Item=u64> {
    //fn advance(&self, ref: u64);
}


pub trait IndexStore<'a> {
    type Reader: IndexReader<'a>;

    fn reader(&'a self) -> Self::Reader;
    fn add_field(&mut self, name: String, field_type: FieldType) -> Result<FieldRef, AddFieldError>;
    fn remove_field(&mut self, field_ref: &FieldRef) -> bool;
    fn insert_or_update_document(&mut self, doc: Document);
    fn remove_document_by_key(&mut self, doc_key: &str) -> bool;
}
