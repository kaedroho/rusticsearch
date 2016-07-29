use search::term::Term;
use search::similarity::{IndexStats, FieldStats, FieldTermStats, SimilarityModel};
use search::store::IndexReader;


#[derive(Debug, PartialEq)]
pub struct TermScorer {
    similarity_model: SimilarityModel,
}


impl TermScorer {
    pub fn score<'a, R: IndexReader<'a>>(&self, index_reader: &'a R, field_name: &str, term: &Term, term_freq: u32) -> f64 {
        let term_bytes = term.to_bytes();

        let index_stats = IndexStats {
            total_docs: index_reader.num_docs() as u64,
        };

        let field_stats = FieldStats {
            average_length: 1.0f64, // TODO
        };

        let field_term_stats = FieldTermStats {
            total_docs: index_reader.term_doc_freq(&term_bytes, field_name),
        };

        self.similarity_model.score(term_freq, &index_stats, &field_stats, &field_term_stats)
    }
}


impl Default for TermScorer {
    fn default() -> TermScorer {
        TermScorer {
            similarity_model: SimilarityModel::TF_IDF,
        }
    }
}
