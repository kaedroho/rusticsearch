use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use analysis::AnalyzerSpec;
use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;


#[derive(Debug)]
pub struct AnalyzerRegistry {
    analyzers: HashMap<String, AnalyzerSpec>,
    tokenizers: HashMap<String, TokenizerSpec>,
    filters: HashMap<String, FilterSpec>,
}


impl AnalyzerRegistry {
    pub fn new() -> AnalyzerRegistry {
        AnalyzerRegistry {
            analyzers: HashMap::new(),
            tokenizers: HashMap::new(),
            filters: HashMap::new(),
        }
    }

    pub fn insert_tokenizer(&mut self, name: String, tokenizer: TokenizerSpec) -> Option<TokenizerSpec> {
        self.tokenizers.insert(name, tokenizer)
    }

    pub fn get_tokenizer(&self, name: &str) -> Option<&TokenizerSpec> {
        self.tokenizers.get(name)
    }

    pub fn tokenizers_len(&self) -> usize {
        self.tokenizers.len()
    }

    pub fn insert_filter(&mut self, name: String, filter: FilterSpec) -> Option<FilterSpec> {
        self.filters.insert(name, filter)
    }

    pub fn get_filter(&self, name: &str) -> Option<&FilterSpec> {
        self.filters.get(name)
    }

    pub fn filters_len(&self) -> usize {
        self.filters.len()
    }

    fn get_default_analyzer(&self) -> AnalyzerSpec {
        self.get("default").cloned().unwrap_or_else(|| {
            AnalyzerSpec {
                tokenizer: TokenizerSpec::Standard,
                filters: vec![
                    FilterSpec::Lowercase,
                    FilterSpec::ASCIIFolding,
                ]
            }
        })
    }

    pub fn get_default_index_analyzer(&self) -> AnalyzerSpec {
        self.get("default_index").cloned().unwrap_or_else(|| {
            self.get_default_analyzer()
        })
    }

    pub fn get_default_search_analyzer(&self) -> AnalyzerSpec {
        self.get("default_search").cloned().unwrap_or_else(|| {
            self.get_default_analyzer()
        })
    }
}


impl Deref for AnalyzerRegistry {
    type Target = HashMap<String, AnalyzerSpec>;

    fn deref(&self) -> &HashMap<String, AnalyzerSpec> {
        &self.analyzers
    }
}


impl DerefMut for AnalyzerRegistry {
    fn deref_mut(&mut self) -> &mut HashMap<String, AnalyzerSpec> {
        &mut self.analyzers
    }
}
