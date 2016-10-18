#[derive(Debug, Clone, PartialEq)]
pub enum SimilarityModel {
    TfIdf,
    Bm25{k1: f64, b: f64},
}


/// tf(term_frequency) = log(term_frequency + 1.0) + 1.0
#[inline]
fn tf(term_frequency: u32) -> f64 {
    (term_frequency as f64 + 1.0f64).ln() + 1.0
}


/// idf(term_docs, total_docs) = log((total_docs + 1.0) / (term_docs + 1.0)) + 1.0
#[inline]
fn idf(term_docs: u64, total_docs: u64) -> f64 {
    ((total_docs as f64 + 1.0) / (term_docs as f64 + 1.0)).ln() + 1.0
}


impl SimilarityModel {
    pub fn score(&self, term_frequency: u32, length: f64, total_tokens: u64, total_docs: u64, total_docs_with_term: u64) -> f64 {
        match *self {
            SimilarityModel::TfIdf => {
                let tf = tf(term_frequency);
                let idf = idf(total_docs_with_term, total_docs);

                tf * idf
            }
            SimilarityModel::Bm25{k1, b} => {
                let tf = tf(term_frequency);
                let idf = idf(total_docs_with_term, total_docs);
                let average_length = (total_tokens as f64 + 1.0f64) / (total_docs as f64 + 1.0f64);

                idf * (k1 + 1.0) * (tf / (tf + (k1 * ((1.0 - b) + b * length.sqrt() / average_length.sqrt())) + 1.0f64))
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::SimilarityModel;

    #[test]
    fn test_tf_idf_higher_term_freq_increases_score() {
        let similarity = SimilarityModel::TfIdf;

        assert!(similarity.score(2, 40, 100, 10, 5) > similarity.score(1, 40, 100, 10, 5));
    }

    #[test]
    fn test_tf_idf_lower_term_docs_increases_score() {
        let similarity = SimilarityModel::TfIdf;

        assert!(similarity.score(1, 40, 100, 10, 5) > similarity.score(1, 40, 100, 10, 10));
    }

    #[test]
    fn test_tf_idf_field_length_doesnt_affect_score() {
        let similarity = SimilarityModel::TfIdf;

        assert!(similarity.score(1, 100, 100, 20, 5) == similarity.score(1, 40, 100, 20, 5));
    }

    #[test]
    fn test_tf_idf_total_tokens_doesnt_affect_score() {
        let similarity = SimilarityModel::TfIdf;

        assert!(similarity.score(1, 40, 1000, 20, 5) == similarity.score(1, 40, 100, 20, 5));
    }

    #[test]
    fn test_tf_idf_handles_zeros() {
        let similarity = SimilarityModel::TfIdf;

        assert!(similarity.score(0, 0, 0, 0, 0).is_finite());
    }

    #[test]
    fn test_bm25_higher_term_freq_increases_score() {
        let similarity = SimilarityModel::Bm25 {
            k1: 1.2,
            b: 0.75,
        };

        assert!(similarity.score(2, 40, 100, 10, 5) > similarity.score(1, 40, 100, 10, 5));
    }

    #[test]
    fn test_bm25_lower_term_docs_increases_score() {
        let similarity = SimilarityModel::Bm25 {
            k1: 1.2,
            b: 0.75,
        };

        assert!(similarity.score(1, 40, 100, 10, 5) > similarity.score(1, 40, 100, 10, 10));
    }

    #[test]
    fn test_bm25_lower_field_length_increases_score() {
        let similarity = SimilarityModel::Bm25 {
            k1: 1.2,
            b: 0.75,
        };

        assert!(similarity.score(1, 40, 100, 20, 5) > similarity.score(1, 100, 100, 20, 5));
    }

    #[test]
    fn test_bm25_higher_total_tokens_increases_score() {
        let similarity = SimilarityModel::Bm25 {
            k1: 1.2,
            b: 0.75,
        };

        assert!(similarity.score(1, 40, 1000, 20, 5) > similarity.score(1, 40, 100, 20, 5));
    }

    #[test]
    fn test_bm25_handles_zeros() {
        let similarity = SimilarityModel::Bm25 {
            k1: 0.0,
            b: 0.0,
        };

        assert!(similarity.score(0, 0, 0, 0, 0).is_finite());
    }
}
