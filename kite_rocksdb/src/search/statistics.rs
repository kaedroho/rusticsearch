use std::collections::HashMap;

use kite::schema::FieldRef;

use RocksDBIndexReader;
use segment::{Segment, SegmentReadError};
use term_dictionary::TermRef;
use key_builder::KeyBuilder;


pub struct RocksDBStatisticsReader<'a> {
    index_reader: &'a RocksDBIndexReader<'a>,
    total_docs: HashMap<FieldRef, i64>,
    total_tokens: HashMap<FieldRef, i64>,
    term_document_frequencies: HashMap<(FieldRef, TermRef), i64>,
}


impl<'a> RocksDBStatisticsReader<'a> {
    pub fn new(index_reader: &'a RocksDBIndexReader) -> RocksDBStatisticsReader<'a> {
        RocksDBStatisticsReader {
            index_reader: index_reader,
            total_docs: HashMap::new(),
            total_tokens: HashMap::new(),
            term_document_frequencies: HashMap::new(),
        }
    }

    fn get_statistic(&self, name: &[u8]) -> Result<i64, SegmentReadError> {
        let mut val = 0;

        for segment in self.index_reader.store.segments.iter_active(&self.index_reader) {
            if let Some(new_val) = try!(segment.load_statistic(name)) {
                val += new_val;
            }
        }

        Ok(val)
    }

    pub fn total_docs(&mut self, field_ref: FieldRef) -> Result<i64, SegmentReadError> {
        if let Some(val) = self.total_docs.get(&field_ref) {
            return Ok(*val);
        }

        let stat_name = KeyBuilder::segment_stat_total_field_docs_stat_name(field_ref.ord());
        let val = try!(self.get_statistic(&stat_name));
        self.total_docs.insert(field_ref, val);
        Ok(val)
    }

    pub fn total_tokens(&mut self, field_ref: FieldRef) -> Result<i64, SegmentReadError> {
        if let Some(val) = self.total_tokens.get(&field_ref) {
            return Ok(*val);
        }

        let stat_name = KeyBuilder::segment_stat_total_field_tokens_stat_name(field_ref.ord());
        let val = try!(self.get_statistic(&stat_name));
        self.total_tokens.insert(field_ref, val);
        Ok(val)
    }

    pub fn term_document_frequency(&mut self, field_ref: FieldRef, term_ref: TermRef) -> Result<i64, SegmentReadError> {
        if let Some(val) = self.term_document_frequencies.get(&(field_ref, term_ref)) {
            return Ok(*val);
        }

        let stat_name = KeyBuilder::segment_stat_term_doc_frequency_stat_name(field_ref.ord(), term_ref.ord());
        let val = try!(self.get_statistic(&stat_name));
        self.term_document_frequencies.insert((field_ref, term_ref), val);
        Ok(val)
    }
}
