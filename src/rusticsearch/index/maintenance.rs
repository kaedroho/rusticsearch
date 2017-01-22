use index::Index;


impl Index {
    /// Run a maintenance task on the index
    /// This must be run periodically by a background thread. It is not currently thread-safe
    pub fn run_maintenance_task(&self) -> Result<(), String> {
        let segment_stats = try!(self.store.get_segment_statistics());

        // TODO: Deactivate segments with 100% deletions
        // TODO: Vacuum segments with many deletions

        // Merge segments

        // Firstly we classify each active segment into one of 5 groups (based on the number of
        // total documents they have):
        // Group 1: 1 - 9 docs
        // Group 2: 10 - 99 docs
        // Group 3: 100 - 999 docs
        // Group 4: 1000 - 9999 docs
        // Group 5: 10000 - 65536 docs

        // The group with the most active segments can perform a merge. A merge can be done on
        // between 5 - 1000 segments at a time. The smallest segments get merged first.

        let mut segments_g1 = Vec::new();
        let mut segments_g2 = Vec::new();
        let mut segments_g3 = Vec::new();
        let mut segments_g4 = Vec::new();
        let mut segments_g5 = Vec::new();

        for (segment, stats) in segment_stats {
            match stats.total_docs() {
                1 ... 9 => segments_g1.push((segment, stats)),
                10 ... 99 => segments_g2.push((segment, stats)),
                100 ... 999 => segments_g3.push((segment, stats)),
                1000 ... 9999 => segments_g4.push((segment, stats)),
                10000 ... 65536 => segments_g5.push((segment, stats)),
                _ => {},
            }
        }

        // Now sort the groups by length in ascending order
        let mut segments_grouped = vec![segments_g1, segments_g2, segments_g3, segments_g4, segments_g5];
        segments_grouped.sort_by_key(|group| group.len());

        // The group with the most segments is our merge candidate. Check that it has above the
        // minimum number of documents to start a merge and truncate it to be less than the maximum.
        let mut group_to_merge = segments_grouped.pop().unwrap();

        if group_to_merge.len() < 3 {
            // No point in merging these
            return Ok(());
        }

        // Now we've found a group of segments to merge, we must check that all the docs will fit in a
        // single segment. If not, we choose the largest sub-group of segments to merge that fills the
        // quota as much as possible

        let mut current_doc_count: u32 = 0;
        let mut segment_ids = Vec::new();

        // Sort segments total_docs in descending order
        // TODO: Check that this is descending order
        group_to_merge.sort_by_key(|&(_, ref stats)| -stats.total_docs());

        for (segment, stats) in group_to_merge {
            if current_doc_count + stats.total_docs() as u32 > 65536 {
                // No space for this segment
                continue;
            }

            segment_ids.push(segment);
            current_doc_count += stats.total_docs() as u32;
        }

        // Merge segments
        try!(self.store.merge_segments(&segment_ids));
        try!(self.store.purge_segments(&segment_ids));

        Ok(())
    }
}
