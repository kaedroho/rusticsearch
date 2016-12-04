use std::str;
use std::collections::{HashMap, BTreeSet};

use rocksdb::{self, WriteBatch, WriteOptions};
use kite::document::DocRef;
use byteorder::{ByteOrder, BigEndian, WriteBytesExt};

use RocksDBIndexStore;
use key_builder::KeyBuilder;
use doc_id_set::DocIdSet;


#[derive(Debug)]
pub enum SegmentMergeError {
    TooManyDocs,
    RocksDBError(rocksdb::Error),
}


impl From<rocksdb::Error> for SegmentMergeError {
    fn from(e: rocksdb::Error) -> SegmentMergeError {
        SegmentMergeError::RocksDBError(e)
    }
}


impl From<SegmentMergeError> for String {
    fn from(e: SegmentMergeError) -> String {
        match e {
            SegmentMergeError::TooManyDocs => "Too many docs".to_string(),
            SegmentMergeError::RocksDBError(e) => e.into(),
        }
    }
}


impl RocksDBIndexStore {
    fn merge_segment_data(&self, source_segments: &Vec<u32>, dest_segment: u32, doc_ref_mapping: &HashMap<DocRef, u16>) -> Result<(), SegmentMergeError> {
        // Put source_segments in a BTreeSet as this is much faster for performing contains queries against
        let source_segments_btree = source_segments.iter().collect::<BTreeSet<_>>();

        // Since we're merging existing data, there's no need to recover if it crashes half way through
        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);

        // Merge the term directories
        // The term directory keys are ordered to be most convenient for retrieving all the segments
        // of for a term/field combination in one go (field/term/segment). So we don't end up pulling
        // in a lot of unwanted data, we firstly iterate the keys, it they one of the source segments
        // looking for then we load them and append them to our new segment.

        /// Converts term directory key strings "d1/2/3" into tuples of 3 i32s (1, 2, 3)
        fn parse_term_directory_key(key: &[u8]) -> (u32, u32, u32) {
            let mut nums_iter = key[1..].split(|b| *b == b'/').map(|s| str::from_utf8(s).unwrap().parse::<u32>().unwrap());
            (nums_iter.next().unwrap(), nums_iter.next().unwrap(), nums_iter.next().unwrap())
        }

        let mut current_td_key: Option<(u32, u32)> = None;
        let mut current_td = Vec::new();

        let mut iter = self.db.iterator();
        iter.seek(b"d");
        while iter.next() {
            let k = iter.key().unwrap();

            if k[0] != b'd' {
                // No more term directories to merge
                break;
            }

            let (field, term, segment) = parse_term_directory_key(&k);

            if source_segments_btree.contains(&segment) {
                if current_td_key != Some((field, term)) {
                    // Finished current term directory. Write it to the DB and start the next one
                    if let Some((field, term)) = current_td_key {
                        let kb = KeyBuilder::segment_dir_list(dest_segment, field, term);
                        try!(self.db.put_opt(&kb.key(), &current_td, &write_options));
                        current_td.clear();
                    }

                    current_td_key = Some((field, term));
                }

                // Merge term directory into the new one (and remap the doc ids)
                for doc_id in DocIdSet::from_bytes(iter.value().unwrap().to_vec()).iter() {
                    let doc_ref = DocRef::from_segment_ord(segment, doc_id);
                    let new_doc_id = doc_ref_mapping.get(&doc_ref).unwrap();
                    current_td.write_u16::<BigEndian>(*new_doc_id).unwrap();
                }
            }
        }

        // All done, write the last term directory
        if let Some((field, term)) = current_td_key {
            let kb = KeyBuilder::segment_dir_list(dest_segment, field, term);
            try!(self.db.put_opt(&kb.key(), &current_td, &write_options));
            current_td.clear();
        }

        // Merge the stored values
        // All stored value keys start with the segment id. So we need to:
        // - Iterate all stored value keys that are prefixed by one of the stored segment ids
        // - Remap their doc ids to the one in the new segment
        // - Write the value back with the new segment/doc ids in the key

        /// Converts stored value key strings "v1/2/3/v" into tuples of 3 i32s and a Vec<u8> (1, 2, 3, vec![b'v', b'a', b'l'])
        fn parse_stored_value_key(key: &[u8]) -> (u32, u32, u32, Vec<u8>) {
            let mut parts_iter = key[1..].split(|b| *b == b'/');
            let segment = str::from_utf8(parts_iter.next().unwrap()).unwrap().parse::<u32>().unwrap();
            let doc_id = str::from_utf8(parts_iter.next().unwrap()).unwrap().parse::<u32>().unwrap();
            let field_ord = str::from_utf8(parts_iter.next().unwrap()).unwrap().parse::<u32>().unwrap();
            let value_type = parts_iter.next().unwrap().to_vec();

            (segment, doc_id, field_ord, value_type)
        }

        for source_segment in source_segments.iter() {
            let kb = KeyBuilder::segment_stored_values_prefix(*source_segment);
            let mut iter = self.db.iterator();
            iter.seek(&kb.key());
            while iter.next() {
                let k = iter.key().unwrap();

                if k[0] != b'v' {
                    // No more stored values to move
                    break;
                }

                let (segment, doc_id, field, value_type) = parse_stored_value_key(&k);

                if segment != *source_segment {
                    // Segment finished
                    break;
                }

                // Remap doc id
                let doc_ref = DocRef::from_segment_ord(segment, doc_id as u16);
                let new_doc_id = doc_ref_mapping.get(&doc_ref).unwrap();

                // Write value into new segment
                let kb = KeyBuilder::stored_field_value(dest_segment, *new_doc_id, field, &value_type);
                try!(self.db.put_opt(&kb.key(), &iter.value().unwrap(), &write_options));
            }
        }

        // Merge the statistics
        // Like stored values, these start with segment ids. But instead of just rewriting the
        // key, we need to sum up all the statistics across the segments being merged.

        let mut statistics = HashMap::new();

        /// Converts statistic key strings "s1/total_docs" into tuples of 1 i32 and a Vec<u8> (1, ['t', 'o', 't', ...])
        fn parse_statistic_key(key: &[u8]) -> (u32, Vec<u8>) {
            let mut parts_iter = key[1..].split(|b| *b == b'/');
            let segment = str::from_utf8(parts_iter.next().unwrap()).unwrap().parse::<u32>().unwrap();
            let statistic_name = parts_iter.next().unwrap().to_vec();

            (segment, statistic_name)
        }

        // Fetch and merge statistics
        for source_segment in source_segments.iter() {
            let kb = KeyBuilder::segment_stat_prefix(*source_segment);
            let mut iter = self.db.iterator();
            iter.seek(&kb.key());
            while iter.next() {
                let k = iter.key().unwrap();
                if k[0] != b's' {
                    // No more statistics to merge
                    break;
                }

                let (segment, statistic_name) = parse_statistic_key(&k);

                if segment != *source_segment {
                    // Segment finished
                    break;
                }


                let mut stat = statistics.entry(statistic_name).or_insert(0);
                *stat += BigEndian::read_i64(&iter.value().unwrap());
            }
        }

        // Write merged statistics to new segment
        for (stat_name, stat_value) in statistics {
            let kb = KeyBuilder::segment_stat(dest_segment, &stat_name);
            let mut val_bytes = [0; 8];
            BigEndian::write_i64(&mut val_bytes, stat_value);
            try!(self.db.put_opt(&kb.key(), &val_bytes, &write_options));
        }

        // Note: Don't merge the deletion lists
        // Deletion lists can change at any time so we must lock the "document index"
        // before merging them so they can't be altered during merge. we cannot lock
        // this until the commit phase though.

        Ok(())
    }

    fn commit_segment_merge(&self, source_segments: &Vec<u32>, dest_segment: u32, doc_ref_mapping: &HashMap<DocRef, u16>) -> Result<(), SegmentMergeError> {
        let mut write_batch = WriteBatch::default();

        // Activate new segment
        let kb = KeyBuilder::segment_active(dest_segment);
        try!(write_batch.put(&kb.key(), b""));

        // Deactivate old segments
        for source_segment in source_segments.iter() {
            // Activate new segment
            let kb = KeyBuilder::segment_active(*source_segment);
            try!(write_batch.delete(&kb.key()));
        }

        // Update document index and commit
        // This will write the write batch
        try!(self.document_index.commit_segment_merge(&self.db, write_batch, source_segments, dest_segment, doc_ref_mapping));

        Ok(())
    }

    pub fn merge_segments(&self, source_segments: &Vec<u32>) -> Result<u32, SegmentMergeError> {
        let dest_segment = try!(self.segments.new_segment(&self.db));

        // Generate a mapping between the ids of the documents in the old segments to the new one
        // This packs the id spaces of the old segments together:
        // For example, say we have to merge 3 segments with 100 documents each:
        //  - The first segment's ids will be the same as before
        //  - The second segment's ids will be remapped to 100 - 199
        //  - The third segment's ids will be remapped to 200 - 299

        let mut doc_ref_mapping: HashMap<DocRef, u16> = HashMap::new();
        let mut current_ord: u32 = 0;

        for source_segment in source_segments.iter() {
            let kb = KeyBuilder::segment_stat(*source_segment, b"total_docs");
            let total_docs = match try!(self.db.get(&kb.key())) {
                Some(total_docs_bytes) => {
                    BigEndian::read_i64(&total_docs_bytes)
                }
                None => continue,
            };

            for source_ord in 0..total_docs {
                if current_ord >= 65536 {
                    return Err(SegmentMergeError::TooManyDocs);
                }

                let from = DocRef::from_segment_ord(*source_segment, source_ord as u16);
                doc_ref_mapping.insert(from, current_ord as u16);
                current_ord += 1;
            }
        }

        // Merge segment data
        // Most of the heavy lifting happens here. This merges all the immutable parts of
        // the segment (which is everything but the deletion list). It does not activate the
        // segment.
        // This means that nothing bad will happen if it crashes half way through -- the
        // worst that could happen is we're left with a partially-written segment that we
        // have to clean up.
        try!(self.merge_segment_data(&source_segments, dest_segment, &doc_ref_mapping));

        // Commit the merge
        // This activates the new segment and updates the document index. Effectively committing
        // the merge.
        // Throughout this stage we need an exclusive lock to the document index. This is to
        // prevent documents in the source segments being deleted/updated so we don't accidentally
        // undelete them (this will block until the merge is complete so they delete/update from
        // the new segment).
        try!(self.commit_segment_merge(&source_segments, dest_segment, &doc_ref_mapping));

        Ok(dest_segment)
    }

    pub fn purge_segments(&self, segments: &Vec<u32>) -> Result<(), rocksdb::Error> {
        // Put segments in a BTreeSet as this is much faster for performing contains queries against
        let segments_btree = segments.iter().collect::<BTreeSet<_>>();

        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);

        // Purge term directories

        /// Converts term directory key strings "d1/2/3" into tuples of 3 i32s (1, 2, 3)
        fn parse_term_directory_key(key: &[u8]) -> (u32, u32, u32) {
            let mut nums_iter = key[1..].split(|b| *b == b'/').map(|s| str::from_utf8(s).unwrap().parse::<u32>().unwrap());
            (nums_iter.next().unwrap(), nums_iter.next().unwrap(), nums_iter.next().unwrap())
        }

        let mut iter = self.db.iterator();
        iter.seek(b"d");
        while iter.next() {
            let k = iter.key().unwrap();

            if k[0] != b'd' {
                // No more term directories to delete
                break;
            }

            let (_, _, segment) = parse_term_directory_key(&k);

            if segments_btree.contains(&segment) {
                try!(self.db.delete(&k));
            }
        }


        // Purge the stored values

        /// Converts stored value key strings "v1/2/3/v" into tuples of 3 i32s and a Vec<u8> (1, 2, 3, vec![b'v', b'a', b'l'])
        fn parse_stored_value_key(key: &[u8]) -> (u32, u32, u32, Vec<u8>) {
            let mut parts_iter = key[1..].split(|b| *b == b'/');
            let segment = str::from_utf8(parts_iter.next().unwrap()).unwrap().parse::<u32>().unwrap();
            let doc_id = str::from_utf8(parts_iter.next().unwrap()).unwrap().parse::<u32>().unwrap();
            let field_ord = str::from_utf8(parts_iter.next().unwrap()).unwrap().parse::<u32>().unwrap();
            let value_type = parts_iter.next().unwrap().to_vec();

            (segment, doc_id, field_ord, value_type)
        }

        for source_segment in segments.iter() {
            let kb = KeyBuilder::segment_stored_values_prefix(*source_segment);
            let mut iter = self.db.iterator();
            iter.seek(&kb.key());
            while iter.next() {
                let k = iter.key().unwrap();

                if k[0] != b'v' {
                    // No more stored values to delete
                    break;
                }

                let (segment, _, _, _) = parse_stored_value_key(&k);

                if segment != *source_segment {
                    // Segment finished
                    break;
                }

                try!(self.db.delete_opt(&k, &write_options));
            }
        }

        // Purge the statistics

        /// Converts statistic key strings "s1/total_docs" into tuples of 1 i32 and a Vec<u8> (1, ['t', 'o', 't', ...])
        fn parse_statistic_key(key: &[u8]) -> (u32, Vec<u8>) {
            let mut parts_iter = key[1..].split(|b| *b == b'/');
            let segment = str::from_utf8(parts_iter.next().unwrap()).unwrap().parse::<u32>().unwrap();
            let statistic_name = parts_iter.next().unwrap().to_vec();

            (segment, statistic_name)
        }

        for source_segment in segments.iter() {
            let kb = KeyBuilder::segment_stat_prefix(*source_segment);
            let mut iter = self.db.iterator();
            iter.seek(&kb.key());
            while iter.next() {
                let k = iter.key().unwrap();

                if k[0] != b's' {
                    // No more statistics to purge
                    break;
                }

                let (segment, _) = parse_statistic_key(&k);

                if segment != *source_segment {
                    // Segment finished
                    break;
                }

                try!(self.db.delete_opt(&k, &write_options));
            }
        }

        // Purge the deletion lists
        for source_segment in segments.iter() {
            let kb = KeyBuilder::segment_del_list(*source_segment);
            try!(self.db.delete_opt(&kb.key(), &write_options));
        }

        Ok(())
    }
}
