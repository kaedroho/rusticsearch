use roaring::RoaringBitmap;

use search::schema::FieldId;
use search::term::TermId;
use search::document::DocId;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SegmentId(pub u32);

pub trait Segment {
    fn load_statistic(&self, stat_name: &[u8]) -> Result<Option<i64>, String>;
    fn load_stored_field_value_raw(&self, doc_local_id: u16, field_id: FieldId, value_type: &[u8]) -> Result<Option<Vec<u8>>, String>;
    fn load_postings_list(&self, field_id: FieldId, term_id: TermId) -> Result<Option<RoaringBitmap>, String>;
    fn load_deletion_list(&self) -> Result<Option<RoaringBitmap>, String>;
    fn id(&self) -> SegmentId;

    fn doc_id(&self, local_id: u16) -> DocId {
        DocId(self.id(), local_id)
    }
}
