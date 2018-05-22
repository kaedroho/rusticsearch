use std::cmp::Ordering;
use std::collections::BinaryHeap;

use search::collectors::{Collector, DocumentMatch};

/// An f32 that cannot be NaN.
/// We need to order documents by score but NaN cannot be ordered, so we convert all scores into
/// Realf32 first, handling any invalid values while doing that conversion
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
struct RealF32(f32);

impl RealF32 {
    fn new(val: f32) -> Option<RealF32> {
        if val.is_nan() {
            None
        } else {
            Some(RealF32(val))
        }
    }
}

impl Eq for RealF32 {}

impl Ord for RealF32 {
    fn cmp(&self, other: &RealF32) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct ScoredDocument {
    id: u64,
    score: RealF32,
}

impl Ord for ScoredDocument {
    fn cmp(&self, other: &ScoredDocument) -> Ordering {
        self.score.cmp(&other.score)
    }
}

impl PartialOrd for ScoredDocument {
    fn partial_cmp(&self, other: &ScoredDocument) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct TopScoreCollector {
    max_docs: usize,
    heap: BinaryHeap<ScoredDocument>,
}

impl TopScoreCollector {
    pub fn new(max_docs: usize) -> TopScoreCollector {
        TopScoreCollector {
            max_docs: max_docs,
            heap: BinaryHeap::with_capacity(max_docs + 1),
        }
    }

    pub fn into_sorted_vec(self) -> Vec<DocumentMatch> {
        self.heap.into_sorted_vec().iter()
            .map(|scored_document| {
                DocumentMatch::new_scored(scored_document.id, -scored_document.score.0)
            })
            .collect()
    }
}

impl Collector for TopScoreCollector {
    fn needs_score(&self) -> bool {
        true
    }

    fn collect(&mut self, doc: DocumentMatch) {
        let doc_id = doc.doc_id();
        let score = doc.score();

        // Build a ScoredDocument object, checking that the score is set and not NaN
        let scored_document = match score {
            Some(score) => {
                // Convert to RealF32 which is orderable but does not support NaN
                match RealF32::new(-score) {
                    Some(real_score) => {
                        ScoredDocument {
                            id: doc_id,
                            score: real_score,
                        }
                    }
                    None => {
                        // Score was NaN
                        panic!("document with 'NaN' score was passed into TopScoreCollector");
                    }
                }
            }
            None => {
                panic!("unscored document was passed into TopScoreCollector");
            }
        };

        // Now insert the document into the heap
        self.heap.push(scored_document);

        // Now reduce the heap size if it's too big
        if self.heap.len() > self.max_docs {
            self.heap.pop();
        }
    }
}

#[cfg(test)]
mod tests {
    use search::collectors::{Collector, DocumentMatch};
    use super::TopScoreCollector;

    #[test]
    fn test_top_score_collector_inital_state() {
        let collector = TopScoreCollector::new(10);

        let docs = collector.into_sorted_vec();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_top_score_collector_needs_score() {
        let collector = TopScoreCollector::new(10);

        assert_eq!(collector.needs_score(), true);
    }

    #[test]
    fn test_top_score_collector_collect() {
        let mut collector = TopScoreCollector::new(10);

        collector.collect(DocumentMatch::new_scored(0, 1.0f32));
        collector.collect(DocumentMatch::new_scored(1, 0.5f32));
        collector.collect(DocumentMatch::new_scored(2, 2.0f32));
        collector.collect(DocumentMatch::new_scored(3, 1.5f32));

        let docs = collector.into_sorted_vec();
        assert_eq!(docs.len(), 4);
        assert_eq!(docs[0].id, 2);
        assert_eq!(docs[1].id, 3);
        assert_eq!(docs[2].id, 0);
        assert_eq!(docs[3].id, 1);
    }

    #[test]
    fn test_top_score_collector_truncate() {
        let mut collector = TopScoreCollector::new(2);

        collector.collect(DocumentMatch::new_scored(0, 1.0f32));
        collector.collect(DocumentMatch::new_scored(1, 0.5f32));
        collector.collect(DocumentMatch::new_scored(2, 2.0f32));

        let docs = collector.into_sorted_vec();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].id, 2);
        assert_eq!(docs[1].id, 0);
    }
}
