use std::cmp::Ordering;

use store::IndexReader;
use query::Query;
use response::{SearchResponse, SearchHit};
use query_set::build_iterator_from_query;


#[derive(Debug)]
pub struct SearchRequest {
    pub query: Query,
    pub from: usize,
    pub size: usize,
    pub terminate_after: Option<usize>,
}

impl SearchRequest {
    pub fn run<'a, R: IndexReader<'a>>(&self, index_reader: &'a R) -> SearchResponse<'a> {
        // Find all hits
        let mut hits = Vec::new();
        let iterator = build_iterator_from_query(index_reader, &self.query);

        for doc_id in iterator {
            if let Some(doc) = index_reader.get_document_by_id(&doc_id) {
                if let Some(score) = self.query.rank(index_reader, &doc) {
                    hits.push(SearchHit {
                        doc: &doc,
                        score: score,
                    });
                }
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
