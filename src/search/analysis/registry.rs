use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use search::analysis::Analyzer;


#[derive(Debug)]
pub struct AnalyzerRegistry {
    analyzers: HashMap<String, Analyzer>,
}


impl AnalyzerRegistry {
    pub fn new() -> AnalyzerRegistry {
        AnalyzerRegistry {
            analyzers: HashMap::new(),
        }
    }
}


impl Deref for AnalyzerRegistry {
    type Target = HashMap<String, Analyzer>;

    fn deref(&self) -> &HashMap<String, Analyzer> {
        &self.analyzers
    }
}


impl DerefMut for AnalyzerRegistry {
    fn deref_mut(&mut self) -> &mut HashMap<String, Analyzer> {
        &mut self.analyzers
    }
}
