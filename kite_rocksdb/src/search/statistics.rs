use std::collections::HashMap;

use kite::schema::FieldRef;
use byteorder::{ByteOrder, BigEndian};

use RocksDBIndexReader;
use term_dictionary::TermRef;
use key_builder::KeyBuilder;


pub struct StatisticsReader<'a> {
    index_reader: &'a RocksDBIndexReader<'a>,
    total_docs: HashMap<FieldRef, i64>,
    total_tokens: HashMap<FieldRef, i64>,
    term_document_frequencies: HashMap<(FieldRef, TermRef), i64>,
}


impl<'a> StatisticsReader<'a> {
    pub fn new(index_reader: &'a RocksDBIndexReader) -> StatisticsReader<'a> {
        StatisticsReader {
            index_reader: index_reader,
            total_docs: HashMap::new(),
            total_tokens: HashMap::new(),
            term_document_frequencies: HashMap::new(),
        }
    }

    fn get_statistic(&self, name: &[u8]) -> i64 {
        let mut val = 0;

        for chunk in self.index_reader.store.chunks.iter_active(&self.index_reader.snapshot) {
            let kb = KeyBuilder::chunk_stat(chunk, name);
            match self.index_reader.snapshot.get(&kb.key()) {
                Ok(Some(new_val)) => {
                    val += BigEndian::read_i64(&new_val);
                }
                Ok(None) => {},
                Err(e) => {},  // FIXME
            }
        }

        val
    }

    pub fn total_docs(&mut self, field_ref: FieldRef) -> i64 {
        if let Some(val) = self.total_docs.get(&field_ref) {
            return *val;
        }

        let stat_name = KeyBuilder::chunk_stat_total_field_docs_stat_name(field_ref.ord());
        let val = self.get_statistic(&stat_name);
        self.total_docs.insert(field_ref, val);
        val
    }

    pub fn total_tokens(&mut self, field_ref: FieldRef) -> i64 {
        if let Some(val) = self.total_tokens.get(&field_ref) {
            return *val;
        }

        let stat_name = KeyBuilder::chunk_stat_total_field_tokens_stat_name(field_ref.ord());
        let val = self.get_statistic(&stat_name);
        self.total_tokens.insert(field_ref, val);
        val
    }

    pub fn term_document_frequency(&mut self, field_ref: FieldRef, term_ref: TermRef) -> i64 {
        if let Some(val) = self.term_document_frequencies.get(&(field_ref, term_ref)) {
            return *val;
        }

        let stat_name = KeyBuilder::chunk_stat_term_doc_frequency_stat_name(field_ref.ord(), term_ref.ord());
        let val = self.get_statistic(&stat_name);
        self.term_document_frequencies.insert((field_ref, term_ref), val);
        val
    }
}
