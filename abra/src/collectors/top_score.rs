use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::binary_heap::{Iter as BinaryHeapIter};

use collectors::{Collector, DocumentMatch};


/// An f64 that cannot be NaN.
/// We need to order documents by score but NaN cannot be ordered, so we convert all scores into
/// RealF64 first, handling any invalid values while doing that conversion
#[derive(Copy, Clone, PartialEq, PartialOrd)]
struct RealF64(f64);

impl RealF64 {
    fn new(val: f64) -> Option<RealF64> {
        if val.is_nan() {
            None
        } else {
            Some(RealF64(val))
        }
    }
}

impl Eq for RealF64 {}

impl Ord for RealF64 {
    fn cmp(&self, other: &RealF64) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}


#[derive(Copy, Clone, PartialEq, Eq)]
struct ScoredDocument {
    id: u64,
    score: RealF64,
}


impl Ord for ScoredDocument {
    fn cmp(&self, other: &ScoredDocument) -> Ordering {
        // Notice that the we flip the ordering here
        other.score.cmp(&self.score)
    }
}

impl PartialOrd for ScoredDocument {
    fn partial_cmp(&self, other: &ScoredDocument) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


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

    pub fn iter<'a>(&'a mut self) -> Iter<'a> {
        Iter {
            heap_iter: self.heap.iter(),
        }
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
                // Convert to RealF64 which is orderable but does not support NaN
                match RealF64::new(score) {
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


pub struct Iter<'a> {
    heap_iter: BinaryHeapIter<'a, ScoredDocument>,
}


impl<'a> Iterator for Iter<'a> {
    type Item = DocumentMatch;

    fn next(&mut self) -> Option<DocumentMatch> {
        match self.heap_iter.next_back() {
            Some(scored_document) => {
                // Convert ScoredDocument back into DocumentMatch
                Some(DocumentMatch::new_scored(scored_document.id, scored_document.score.0))
            }
            None => None
        }
    }
}


#[cfg(test)]
mod tests {
    use collectors::{Collector, DocumentMatch};
    use super::TopScoreCollector;


    #[test]
    fn test_top_score_collector_inital_state() {
        let mut collector = TopScoreCollector::new(10);

        let docs = collector.iter().collect::<Vec<_>>();
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

        collector.collect(DocumentMatch::new_scored(0, 1.0f64));
        collector.collect(DocumentMatch::new_scored(1, 0.5f64));
        collector.collect(DocumentMatch::new_scored(2, 2.0f64));

        let docs = collector.iter().collect::<Vec<_>>();
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].id, 2);
        assert_eq!(docs[1].id, 0);
        assert_eq!(docs[2].id, 1);
    }

    #[test]
    fn test_top_score_collector_truncate() {
        let mut collector = TopScoreCollector::new(2);

        collector.collect(DocumentMatch::new_scored(0, 1.0f64));
        collector.collect(DocumentMatch::new_scored(1, 0.5f64));
        collector.collect(DocumentMatch::new_scored(2, 2.0f64));

        let docs = collector.iter().collect::<Vec<_>>();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].id, 2);
        assert_eq!(docs[1].id, 0);
    }
}
