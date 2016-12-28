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
        let mut analyzers = AnalyzerRegistry {
            analyzers: HashMap::new(),
            tokenizers: HashMap::new(),
            filters: HashMap::new(),
        };

        // Builtin tokenizers
        analyzers.insert_tokenizer("standard".to_string(), TokenizerSpec::Standard);

        // Builtin filters
        analyzers.insert_filter("asciifolding".to_string(), FilterSpec::ASCIIFolding);
        analyzers.insert_filter("lowercase".to_string(), FilterSpec::Lowercase);

        // Builtin analyzers
        analyzers.insert_analyzer("standard".to_string(), AnalyzerSpec {
            tokenizer: TokenizerSpec::Standard,
            filters: vec![
                FilterSpec::Lowercase,
                FilterSpec::ASCIIFolding,
            ]
        });

        analyzers
    }

    pub fn insert_analyzer(&mut self, name: String, analyzer: AnalyzerSpec) -> Option<AnalyzerSpec> {
        self.analyzers.insert(name, analyzer)
    }

    pub fn analyzers(&self) -> &HashMap<String, AnalyzerSpec> {
        &self.analyzers
    }

    pub fn insert_tokenizer(&mut self, name: String, tokenizer: TokenizerSpec) -> Option<TokenizerSpec> {
        self.tokenizers.insert(name, tokenizer)
    }

    pub fn tokenizers(&self) -> &HashMap<String, TokenizerSpec> {
        &self.tokenizers
    }

    pub fn insert_filter(&mut self, name: String, filter: FilterSpec) -> Option<FilterSpec> {
        self.filters.insert(name, filter)
    }

    pub fn filters(&self) -> &HashMap<String, FilterSpec> {
        &self.filters
    }

    fn get_default_analyzer(&self) -> AnalyzerSpec {
        self.analyzers().get("default").cloned().unwrap_or_else(|| {
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
        self.analyzers().get("default_index").cloned().unwrap_or_else(|| {
            self.get_default_analyzer()
        })
    }

    pub fn get_default_search_analyzer(&self) -> AnalyzerSpec {
        self.analyzers().get("default_search").cloned().unwrap_or_else(|| {
            self.get_default_analyzer()
        })
    }
}
