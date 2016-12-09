use compact_segment_capnp::compact_segment;
use capnp::serialize_packed;

use kite::document::Document;


#[derive(Debug)]
pub struct RocksDBCompactSegment {
    data: Vec<u8>,
}


pub fn document_into_compact_segment(doc: Document) -> RocksDBCompactSegment {
    let mut message = ::capnp::message::Builder::new_default();
    {
        let compact_segment = message.init_root::<compact_segment::Builder>();
        let mut term_directories = compact_segment.init_term_directories();


/*
        // Insert contents

        // Indexed fields
        let mut term_frequencies = HashMap::new();
        for (field, tokens) in doc.indexed_fields.iter() {
            let mut field_token_count = 0;

            for token in tokens.iter() {
                field_token_count += 1;

                let term_ref = try!(self.term_dictionary.get_or_create(&self.db, &token.term));

                // Term frequency
                let mut term_frequency = term_frequencies.entry(term_ref).or_insert(0);
                *term_frequency += 1;

                // Write directory list
                let kb = KeyBuilder::segment_dir_list(doc_ref.segment(), field.ord(), term_ref.ord());
                let mut doc_id_bytes = [0; 2];
                BigEndian::write_u16(&mut doc_id_bytes, doc_ref.ord());
                try!(write_batch.merge(&kb.key(), &doc_id_bytes));
            }

            // Term frequencies
            for (term_ref, frequency) in term_frequencies.drain() {
                // Write term frequency
                // 1 is by far the most common frequency. At search time, we interpret a missing
                // key as meaning there is a term frequency of 1
                if frequency != 1 {
                    let mut value_type = vec![b't', b'f'];
                    value_type.extend(term_ref.ord().to_string().as_bytes());
                    let kb = KeyBuilder::stored_field_value(doc_ref.segment(), doc_ref.ord(), field.ord(), &value_type);
                    let mut frequency_bytes = [0; 8];
                    BigEndian::write_i64(&mut frequency_bytes, frequency);
                    try!(write_batch.merge(&kb.key(), &frequency_bytes));
                }

                // Increment term document frequency
                let kb = KeyBuilder::segment_stat_term_doc_frequency(doc_ref.segment(), field.ord(), term_ref.ord());
                let mut inc_bytes = [0; 8];
                BigEndian::write_i64(&mut inc_bytes, 1);
                try!(write_batch.merge(&kb.key(), &inc_bytes));
            }

            // Field length
            // Used by the BM25 similarity model
            let length = ((field_token_count as f64).sqrt() - 1.0) * 3.0;
            let length = if length > 255.0 { 255.0 } else { length } as u8;
            if length != 0 {
                let kb = KeyBuilder::stored_field_value(doc_ref.segment(), doc_ref.ord(), field.ord(), b"len");
                try!(write_batch.merge(&kb.key(), &[length]));
            }

            // Increment total field docs
            let kb = KeyBuilder::segment_stat_total_field_docs(doc_ref.segment(), field.ord());
            let mut inc_bytes = [0; 8];
            BigEndian::write_i64(&mut inc_bytes, 1);
            try!(write_batch.merge(&kb.key(), &inc_bytes));

            // Increment total field tokens
            let kb = KeyBuilder::segment_stat_total_field_tokens(doc_ref.segment(), field.ord());
            let mut inc_bytes = [0; 8];
            BigEndian::write_i64(&mut inc_bytes, field_token_count);
            try!(write_batch.merge(&kb.key(), &inc_bytes));
        }

        // Stored fields
        for (field, value) in doc.stored_fields.iter() {
            let kb = KeyBuilder::stored_field_value(doc_ref.segment(), doc_ref.ord(), field.ord(), b"val");
            try!(write_batch.merge(&kb.key(), &value.to_bytes()));
        }

        // Increment total docs
        let kb = KeyBuilder::segment_stat(doc_ref.segment(), b"total_docs");
        let mut inc_bytes = [0; 8];
        BigEndian::write_i64(&mut inc_bytes, 1);
        try!(write_batch.merge(&kb.key(), &inc_bytes));
*/




    }

    let mut data: Vec<u8> = Vec::new();
    serialize_packed::write_message(&mut data, &message);

    RocksDBCompactSegment {
        data: data
    }
}
