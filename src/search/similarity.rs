pub struct IndexStats {
    /// Total number of active documents in the index
    pub total_docs: u64,
}


pub struct FieldStats {
    pub sum_total_term_freq: u64,
}


pub struct FieldTermStats {
    /// Total number of docs that contain this term in this field
    pub total_docs: u64,
}


#[derive(Debug, PartialEq)]
pub enum SimilarityModel {
    TF_IDF,
    BM25{k1: f64, b: f64},
}


/// idf(term_docs, total_docs) = log((total_docs + 1.0) / (term_docs + 1.0)) + 1.0
fn idf(term_docs: u64, total_docs: u64) -> f64 {
    let term_docs = term_docs as f64;
    let total_docs = total_docs as f64;
    ((total_docs + 1.0) / (term_docs + 1.0)).log(10.0) + 1.0
}


impl SimilarityModel {
    pub fn score(&self, term_frequency: u32, doc_length: u32, index_stats: &IndexStats, field_stats: &FieldStats, field_term_stats: &FieldTermStats) -> f64 {
        match *self {
            SimilarityModel::TF_IDF => {
                let tf = (term_frequency as f64).sqrt();
                let idf = idf(field_term_stats.total_docs, index_stats.total_docs);

                tf * idf
            }
            SimilarityModel::BM25{k1, b} => {
                let tf = (term_frequency as f64).sqrt();
                let idf = idf(field_term_stats.total_docs, index_stats.total_docs);

                let norm_value = 1.0f64 /* field boost */ / tf;
                let average_length = field_stats.sum_total_term_freq as f64 / index_stats.total_docs as f64;

                idf * (k1 + 1.0) * (tf / (tf + (k1 * ((1.0 - b) + b * norm_value / average_length))))
            }
        }
    }
}
