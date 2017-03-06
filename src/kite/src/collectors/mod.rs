pub mod total_count;
pub mod top_score;


#[derive(Debug)]
pub struct DocumentMatch {
    id: u64,
    score: Option<f64>,
}


impl DocumentMatch {
    pub fn new_unscored(id: u64) -> DocumentMatch {
        DocumentMatch {
            id: id,
            score: None,
        }
    }

    pub fn new_scored(id: u64, score: f64) -> DocumentMatch {
        DocumentMatch {
            id: id,
            score: Some(score),
        }
    }

    #[inline]
    pub fn doc_id(&self) -> u64 {
        self.id
    }

    #[inline]
    pub fn score(&self) -> Option<f64> {
        self.score
    }
}


pub trait Collector {
    fn needs_score(&self) -> bool;
    fn collect(&mut self, doc: DocumentMatch);
}
