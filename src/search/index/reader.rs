use search::document::Document;


pub trait IndexReader<'a> {
    type AllDocRefIterator: DocRefIterator<'a>;
    type TermDocRefIterator: DocRefIterator<'a>;

    fn get_document_by_key(&self, doc_key: &str) -> Option<&Document>;
    fn get_document_by_id(&self, doc_id: &u64) -> Option<&Document>;
    fn contains_document_key(&self, doc_key: &str) -> bool;
    fn next_doc(&self, term: &[u8], field_name: &str, previous_doc: Option<u64>) -> Option<u64>;
    fn num_docs(&self) -> usize;
    fn iter_docids_all(&'a self) -> Self::AllDocRefIterator;
    fn iter_docids_with_term(&'a self, term: &[u8], field_name: &str) -> Option<Self::TermDocRefIterator>;
    fn iter_terms(&'a self, field_name: &str) -> Option<Box<Iterator<Item=&'a [u8]> + 'a>>;
    //pub fn retrieve_document(&self, &Self::DocRef) -> Document;
}


pub trait DocRefIterator<'a>: Iterator<Item=u64> {
    //fn advance(&self, ref: u64);
}
