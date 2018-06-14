use std::collections::HashMap;

use search::{Document, Term, TermId};
use search::schema::FieldId;
use search::segment::{SegmentId, Segment};
use byteorder::{LittleEndian, WriteBytesExt};
use roaring::RoaringBitmap;
use fnv::FnvHashMap;

use super::key_builder::KeyBuilder;

#[derive(Debug)]
pub struct SegmentBuilder {
    current_doc: u16,
    pub term_dictionary: HashMap<Term, TermId>,
    current_term_id: u32,
    pub term_directories: FnvHashMap<(FieldId, TermId), RoaringBitmap>,
    pub statistics: FnvHashMap<Vec<u8>, i64>,
    pub stored_field_values: FnvHashMap<(FieldId, u16, Vec<u8>), Vec<u8>>,
}

#[derive(Debug)]
pub enum DocumentInsertError {
    /// Segment couldn't hold any more docs
    SegmentFull,
}

impl SegmentBuilder {
    pub fn new() -> SegmentBuilder {
        SegmentBuilder {
            current_doc: 0,
            term_dictionary: HashMap::new(),
            current_term_id: 0,
            term_directories: FnvHashMap::default(),
            statistics: FnvHashMap::default(),
            stored_field_values: FnvHashMap::default(),
        }
    }

    fn get_term_id(&mut self, term: &Term) -> TermId {
        if let Some(term_id) = self.term_dictionary.get(term) {
            return *term_id;
        }

        // Add the term to the dictionary
        let term_id = TermId(self.current_term_id);
        self.current_term_id += 1;
        self.term_dictionary.insert(term.clone(), term_id);

        term_id
    }

    pub fn add_document(&mut self, doc: &Document) -> Result<u16, DocumentInsertError> {
        // Get document ord
        let doc_id = self.current_doc;
        self.current_doc += 1;
        try!(self.current_doc.checked_add(1).ok_or(DocumentInsertError::SegmentFull));

        // Insert indexed fields
        let mut term_frequencies = FnvHashMap::default();
        for (field_id, tokens) in doc.indexed_fields.iter() {
            let mut field_token_count = 0;

            for (term, positions) in tokens.iter() {
                let frequency = positions.len();
                field_token_count += frequency;

                // Get term id
                let term_id = self.get_term_id(term);

                // Term frequency
                let term_frequency = term_frequencies.entry(term_id).or_insert(0);
                *term_frequency += frequency;

                // Write directory list
                self.term_directories.entry((*field_id, term_id)).or_insert_with(RoaringBitmap::new).insert(doc_id as u32);

                // Write term frequency
                // 1 is by far the most common frequency. At search time, we interpret a missing
                // key as meaning there is a term frequency of 1
                if frequency != 1 {
                    let mut value_type = vec![b't', b'f'];
                    value_type.extend(term_id.0.to_string().as_bytes());

                    let mut frequency_bytes: Vec<u8> = Vec::new();
                    frequency_bytes.write_i64::<LittleEndian>(frequency as i64).unwrap();

                    self.stored_field_values.insert((*field_id, doc_id, value_type), frequency_bytes);
                }

                // Increment term document frequency
                let stat_name = KeyBuilder::segment_stat_term_doc_frequency_stat_name(field_id.0, term_id.0);
                let stat = self.statistics.entry(stat_name).or_insert(0);
                *stat += 1;
            }

            // Field length
            // Used by the BM25 similarity model
            let length = ((field_token_count as f32).sqrt() - 1.0) * 3.0;
            let length = if length > 255.0 { 255.0 } else { length } as u8;
            if length != 0 {
                self.stored_field_values.insert((*field_id, doc_id, b"len".to_vec()), vec![length]);
            }

            // Increment total field docs
            {
                let stat_name = KeyBuilder::segment_stat_total_field_docs_stat_name(field_id.0);
                let stat = self.statistics.entry(stat_name).or_insert(0);
                *stat += 1;
            }

            // Increment total field tokens
            {
                let stat_name = KeyBuilder::segment_stat_total_field_tokens_stat_name(field_id.0);
                let stat = self.statistics.entry(stat_name).or_insert(0);
                *stat += field_token_count as i64;
            }
        }

        // Insert stored fields
        for (field, value) in doc.stored_fields.iter() {
            self.stored_field_values.insert((*field, doc_id, b"val".to_vec()), value.to_bytes());
        }

        // Increment total docs
        {
            let stat = self.statistics.entry(b"total_docs".to_vec()).or_insert(0);
            *stat += 1;
        }

        Ok(doc_id)
    }
}

impl Segment for SegmentBuilder {
    fn id(&self) -> SegmentId {
        SegmentId(0)
    }

    fn load_statistic(&self, stat_name: &[u8]) -> Result<Option<i64>, String> {
        Ok(self.statistics.get(stat_name).cloned())
    }

    fn load_stored_field_value_raw(&self, doc_local_id: u16, field_id: FieldId, value_type: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(self.stored_field_values.get(&(field_id, doc_local_id, value_type.to_vec())).cloned())
    }

    fn load_term_directory(&self, field_id: FieldId, term_id: TermId) -> Result<Option<RoaringBitmap>, String> {
        Ok(self.term_directories.get(&(field_id, term_id)).cloned())
    }

    fn load_deletion_list(&self) -> Result<Option<RoaringBitmap>, String> {
        Ok(None)
    }
}
