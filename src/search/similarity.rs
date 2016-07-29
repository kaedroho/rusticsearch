pub struct IndexStats {
    /// Total number of active documents in the index
    pub total_docs: u64,
}


pub struct FieldStats {
    /// The average length of this field across all docs (in token-positions)
    pub average_length: f64,
}


pub struct FieldTermStats {
    /// Total number of docs that contain this term in this field
    pub total_docs: u64,
}


#[derive(Debug, PartialEq)]
pub enum SimilarityModel {
    TF_IDF,
}


/// idf(term_docs, total_docs) = log((total_docs + 1.0) / (term_docs + 1.0)) + 1.0
fn idf(term_docs: u64, total_docs: u64) -> f64 {
    let term_docs = term_docs as f64;
    let total_docs = total_docs as f64;
    ((total_docs + 1.0) / (term_docs + 1.0)).log(10.0) + 1.0
}


impl SimilarityModel {
    pub fn score(&self, term_frequency: u32, index_stats: &IndexStats, field_stats: &FieldStats, field_term_stats: &FieldTermStats) -> f64 {
        match *self {
            SimilarityModel::TF_IDF => {
                let tf = (term_frequency as f64).sqrt();
                let idf = idf(field_term_stats.total_docs, index_stats.total_docs);

                tf * idf
            }
        }
    }
}
