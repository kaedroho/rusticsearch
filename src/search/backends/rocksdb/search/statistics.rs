use fnv::FnvHashMap;

use search::schema::FieldId;
use search::term::TermId;
use search::segment::Segment;

use super::super::RocksDBReader;
use super::super::key_builder::KeyBuilder;

pub trait StatisticsReader {
    fn total_docs(&mut self, field_id: FieldId) -> Result<i64, String>;
    fn total_tokens(&mut self, field_id: FieldId) -> Result<i64, String>;
    fn term_document_frequency(&mut self, field_id: FieldId, term_id: TermId) -> Result<i64, String>;
}

pub struct RocksDBStatisticsReader<'a> {
    index_reader: &'a RocksDBReader<'a>,
    total_docs: FnvHashMap<FieldId, i64>,
    total_tokens: FnvHashMap<FieldId, i64>,
    term_document_frequencies: FnvHashMap<(FieldId, TermId), i64>,
}

impl<'a> RocksDBStatisticsReader<'a> {
    pub fn new(index_reader: &'a RocksDBReader) -> RocksDBStatisticsReader<'a> {
        RocksDBStatisticsReader {
            index_reader: index_reader,
            total_docs: FnvHashMap::default(),
            total_tokens: FnvHashMap::default(),
            term_document_frequencies: FnvHashMap::default(),
        }
    }

    fn get_statistic(&self, name: &[u8]) -> Result<i64, String> {
        let mut val = 0;

        for segment in self.index_reader.store.segments.iter_active(&self.index_reader) {
            if let Some(new_val) = try!(segment.load_statistic(name)) {
                val += new_val;
            }
        }

        Ok(val)
    }
}

impl<'a> StatisticsReader for RocksDBStatisticsReader<'a> {
    fn total_docs(&mut self, field_id: FieldId) -> Result<i64, String> {
        if let Some(val) = self.total_docs.get(&field_id) {
            return Ok(*val);
        }

        let stat_name = KeyBuilder::segment_stat_total_field_docs_stat_name(field_id.0);
        let val = try!(self.get_statistic(&stat_name));
        self.total_docs.insert(field_id, val);
        Ok(val)
    }

    fn total_tokens(&mut self, field_id: FieldId) -> Result<i64, String> {
        if let Some(val) = self.total_tokens.get(&field_id) {
            return Ok(*val);
        }

        let stat_name = KeyBuilder::segment_stat_total_field_tokens_stat_name(field_id.0);
        let val = try!(self.get_statistic(&stat_name));
        self.total_tokens.insert(field_id, val);
        Ok(val)
    }

    fn term_document_frequency(&mut self, field_id: FieldId, term_id: TermId) -> Result<i64, String> {
        if let Some(val) = self.term_document_frequencies.get(&(field_id, term_id)) {
            return Ok(*val);
        }

        let stat_name = KeyBuilder::segment_stat_term_doc_frequency_stat_name(field_id.0, term_id.0);
        let val = try!(self.get_statistic(&stat_name));
        self.term_document_frequencies.insert((field_id, term_id), val);
        Ok(val)
    }
}
