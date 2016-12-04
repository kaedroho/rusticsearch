use schema::FieldRef;
use term::TermRef;
use doc_id_set::DocIdSet;
use document::DocRef;


pub trait Segment {
    fn load_statistic(&self, stat_name: &[u8]) -> Result<Option<i64>, String>;
    fn load_stored_field_value_raw(&self, doc_ord: u16, field_ref: FieldRef, value_type: &[u8]) -> Result<Option<Vec<u8>>, String>;
    fn load_term_directory(&self, field_ref: FieldRef, term_ref: TermRef) -> Result<Option<DocIdSet>, String>;
    fn load_deletion_list(&self) -> Result<Option<DocIdSet>, String>;
    fn id(&self) -> u32;

    fn doc_ref(&self, ord: u16) -> DocRef {
        DocRef::from_segment_ord(self.id(), ord)
    }
}
