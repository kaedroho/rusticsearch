use std::collections::HashMap;

use analysis::AnalyzerSpec;
use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;


#[derive(Debug)]
pub struct IndexSettings {
    pub tokenizers: HashMap<String, TokenizerSpec>,
    pub filters: HashMap<String, FilterSpec>,
    pub analyzers: HashMap<String, AnalyzerSpec>,
}


impl IndexSettings {
    pub fn new() -> IndexSettings {
        IndexSettings {
            tokenizers: HashMap::new(),
            filters: HashMap::new(),
            analyzers: HashMap::new(),
        }
    }
}
