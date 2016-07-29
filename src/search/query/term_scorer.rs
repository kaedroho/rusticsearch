use search::term::Term;
use search::similarity::{IndexStats, FieldStats, FieldTermStats, SimilarityModel};
use search::store::IndexReader;


#[derive(Debug, PartialEq)]
pub struct TermScorer {
    similarity_model: SimilarityModel,
}


impl TermScorer {
    pub fn score<'a, R: IndexReader<'a>>(&self, index_reader: &'a R, field_name: &str, term: &Term, term_freq: u32, doc_length: u32) -> f64 {
        let term_bytes = term.to_bytes();

        let index_stats = IndexStats {
            total_docs: index_reader.num_docs() as u64,
        };

        let field_stats = FieldStats {
            sum_total_term_freq: index_reader.sum_total_term_freq(field_name),
        };

        let field_term_stats = FieldTermStats {
            total_docs: index_reader.term_doc_freq(&term_bytes, field_name),
        };

        self.similarity_model.score(term_freq, doc_length, &index_stats, &field_stats, &field_term_stats)
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
