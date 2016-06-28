use std::cmp::Ordering;

use index::Index;
use search::query::Query;
use search::response::{SearchResponse, SearchHit};


#[derive(Debug)]
pub struct SearchRequest {
    pub query: Query,
    pub from: usize,
    pub size: usize,
    pub terminate_after: Option<usize>,
}

impl SearchRequest {
    pub fn run<'a>(&self, index: &'a Index) -> SearchResponse<'a> {
        // Find all hits
        let mut hits = Vec::new();
        for doc in index.store.iter_docs() {
            if let Some(score) = self.query.rank(&doc) {
                hits.push(SearchHit {
                    doc: &doc,
                    score: score,
                });
            }
        }

        // Sort by score
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Less));

        // Pagination
        let total_hits = hits.len();
        if self.from > 0 {
            hits.drain(..self.from);
        }
        hits.truncate(self.size);

        SearchResponse {
            total_hits: total_hits,
            hits: hits,
            terminated_early: false, // TODO
        }
    }
}
