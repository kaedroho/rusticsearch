use search::term::Term;


pub trait IndexReader<'a> {
    type AllDocRefIterator: DocRefIterator<'a>;
    type TermDocRefIterator: DocRefIterator<'a>;

    fn num_docs(&self) -> usize;
    fn iter_docids_all(&'a self) -> Self::AllDocRefIterator;
    fn iter_docids_with_term(&'a self, term: Term, field_name: String) -> Self::TermDocRefIterator;
    //pub fn retrieve_document(&self, &Self::DocRef) -> Document;
}


pub trait DocRefIterator<'a>: Iterator<Item=u64> {
    //fn advance(&self, ref: u64);
}
