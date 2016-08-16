use term::Term;
use similarity::SimilarityModel;
use store::IndexReader;


#[derive(Debug, PartialEq)]
pub struct TermScorer {
    similarity_model: SimilarityModel,
}


impl TermScorer {
    pub fn score<'a, R: IndexReader<'a>>(&self, index_reader: &'a R, field_name: &str, term: &Term, term_frequency: u32, length: u32) -> f64 {
        let term_bytes = term.to_bytes();
        let total_tokens = index_reader.total_tokens(field_name);
        let total_docs = index_reader.num_docs() as u64;
        let total_docs_with_term = index_reader.term_doc_freq(&term_bytes, field_name);

        self.similarity_model.score(term_frequency, length, total_tokens, total_docs, total_docs_with_term)
    }
}


impl Default for TermScorer {
    fn default() -> TermScorer {
        TermScorer {
            similarity_model: SimilarityModel::BM25 {
                k1: 1.2,
                b: 0.75,
            },
        }
    }
}
