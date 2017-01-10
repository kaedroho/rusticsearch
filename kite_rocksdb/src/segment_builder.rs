use std::collections::HashMap;

use kite::{Document, TermRef};
use kite::schema::FieldRef;
use byteorder::{BigEndian, WriteBytesExt};

use key_builder::KeyBuilder;


#[derive(Debug)]
pub struct SegmentBuilder {
    current_doc: u16,
    pub term_dictionary: HashMap<Vec<u8>, TermRef>,
    current_term_ref: u32,
    pub term_directories: HashMap<(FieldRef, TermRef), Vec<u16>>,
    pub statistics: HashMap<Vec<u8>, i64>,
    pub stored_field_values: HashMap<(FieldRef, u16, Vec<u8>), Vec<u8>>,
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
            current_term_ref: 0,
            term_directories: HashMap::new(),
            statistics: HashMap::new(),
            stored_field_values: HashMap::new(),
        }
    }

    // TODO: Need to translate field names to field refs and terms to term refs
    pub fn add_document(&mut self, doc: &Document) -> Result<u16, DocumentInsertError> {
        // Get document ord
        let doc_id = self.current_doc;
        self.current_doc += 1;
        try!(self.current_doc.checked_add(1).ok_or(DocumentInsertError::SegmentFull));

        // Insert indexed fields
        let mut term_frequencies = HashMap::new();
        for (field, tokens) in doc.indexed_fields.iter() {
            let mut field_token_count = 0;

            for token in tokens.iter() {
                field_token_count += 1;

                // Get term ref
                let term_bytes = token.term.as_bytes().to_vec();
                let mut current_term_ref = self.current_term_ref;
                let term_ref = self.term_dictionary.entry(term_bytes).or_insert_with(|| {
                    let term_ref = TermRef::new(current_term_ref);
                    current_term_ref += 1;
                    term_ref
                });
                self.current_term_ref = current_term_ref;

                // Term frequency
                let mut term_frequency = term_frequencies.entry(*term_ref).or_insert(0);
                *term_frequency += 1;

                // Write directory list
                self.term_directories.entry((*field, *term_ref)).or_insert_with(Vec::new).push(doc_id);
            }

            // Term frequencies
            for (term_ref, frequency) in term_frequencies.drain() {
                // Write term frequency
                // 1 is by far the most common frequency. At search time, we interpret a missing
                // key as meaning there is a term frequency of 1
                if frequency != 1 {
                    let mut value_type = vec![b't', b'f'];
                    value_type.extend(term_ref.ord().to_string().as_bytes());

                    let mut frequency_bytes: Vec<u8> = Vec::new();
                    frequency_bytes.write_i64::<BigEndian>(frequency).unwrap();

                    self.stored_field_values.insert((*field, doc_id, value_type), frequency_bytes);
                }

                // Increment term document frequency
                let stat_name = KeyBuilder::segment_stat_term_doc_frequency_stat_name(field.ord(), term_ref.ord());
                let mut stat = self.statistics.entry(stat_name).or_insert(0);
                *stat += 1;
            }

            // Field length
            // Used by the BM25 similarity model
            let length = ((field_token_count as f64).sqrt() - 1.0) * 3.0;
            let length = if length > 255.0 { 255.0 } else { length } as u8;
            if length != 0 {
                self.stored_field_values.insert((*field, doc_id, b"len".to_vec()), vec![length]);
            }

            // Increment total field docs
            {
                let stat_name = KeyBuilder::segment_stat_total_field_docs_stat_name(field.ord());
                let mut stat = self.statistics.entry(stat_name).or_insert(0);
                *stat += 1;
            }

            // Increment total field tokens
            {
                let stat_name = KeyBuilder::segment_stat_total_field_tokens_stat_name(field.ord());
                let mut stat = self.statistics.entry(stat_name).or_insert(0);
                *stat += field_token_count;
            }
        }

        // Insert stored fields
        for (field, value) in doc.stored_fields.iter() {
            self.stored_field_values.insert((*field, doc_id, b"val".to_vec()), value.to_bytes());
        }

        // Increment total docs
        {
            let mut stat = self.statistics.entry(b"total_docs".to_vec()).or_insert(0);
            *stat += 1;
        }

        Ok(doc_id)
    }
}
