use similarity::SimilarityModel;


#[derive(Debug, Clone, PartialEq)]
pub struct TermScorer {
    pub similarity_model: SimilarityModel,
    pub boost: f64,
}


impl TermScorer {
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
