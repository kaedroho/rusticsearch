use std::cmp::Ordering;

use index::Index;
use query::Query;
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
        for (_, doc) in index.docs.iter() {
            if let Some(score) = self.query.rank(&doc) {
                hits.push(SearchHit {
                    doc: &doc,
                    score: score,
                });
            }
        }

        // Sort by score
        hits.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Less));

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
