use term::Term;
use schema::FieldRef;
use similarity::SimilarityModel;
use store::IndexReader;


#[derive(Debug, Clone, PartialEq)]
pub struct TermScorer {
    pub similarity_model: SimilarityModel,
    pub boost: f64,
}


impl TermScorer {
    pub fn score<'a, R: IndexReader<'a>>(&self, index_reader: &'a R, field_ref: &FieldRef, term: &Term, term_frequency: u32, length: u32) -> f64 {
        let term_bytes = term.to_bytes();
        let total_tokens = index_reader.total_tokens(field_ref);
        let total_docs = index_reader.num_docs() as u64;
        let total_docs_with_term = index_reader.num_docs_with_term(&term_bytes, field_ref);

        self.similarity_model.score(term_frequency, length, total_tokens, total_docs, total_docs_with_term)
    }

    pub fn default_with_boost(boost: f64) -> TermScorer {
        TermScorer {
            similarity_model: SimilarityModel::Bm25 {
                k1: 1.2,
                b: 0.75,
            },
            boost: boost,
        }
    }
}


impl Default for TermScorer {
    fn default() -> TermScorer {
        TermScorer::default_with_boost(1.0f64)
    }
}
