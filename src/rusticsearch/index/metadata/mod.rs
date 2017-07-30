pub mod parse;
pub mod file;

use std::collections::{HashMap, BTreeMap};

use serde::{Serialize, Serializer};
use serde_json;

use analysis::AnalyzerSpec;
use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;
use mapping::{Mapping, MappingProperty, FieldMapping};


#[derive(Debug)]
pub struct IndexMetadata {
    analyzers: HashMap<String, AnalyzerSpec>,
    tokenizers: HashMap<String, TokenizerSpec>,
    filters: HashMap<String, FilterSpec>,
    pub mappings: HashMap<String, Mapping>,
}


impl Default for IndexMetadata {
    fn default() -> IndexMetadata {
        let mut metadata = IndexMetadata {
            analyzers: HashMap::new(),
            tokenizers: HashMap::new(),
            filters: HashMap::new(),
            mappings: HashMap::new(),
        };

        // Builtin tokenizers
        metadata.insert_tokenizer("standard".to_string(), TokenizerSpec::Standard);
        metadata.insert_tokenizer("lowercase".to_string(), TokenizerSpec::Lowercase);

        // Builtin filters
        metadata.insert_filter("asciifolding".to_string(), FilterSpec::ASCIIFolding);
        metadata.insert_filter("lowercase".to_string(), FilterSpec::Lowercase);

        // Builtin analyzers
        metadata.insert_analyzer("standard".to_string(), AnalyzerSpec {
            tokenizer: TokenizerSpec::Standard,
            filters: vec![
                FilterSpec::Lowercase,
                FilterSpec::ASCIIFolding,
            ]
        });

        metadata
    }
}


impl IndexMetadata {
    // Tokenizer helpers

    pub fn insert_tokenizer(&mut self, name: String, tokenizer: TokenizerSpec) -> Option<TokenizerSpec> {
        self.tokenizers.insert(name, tokenizer)
    }

    pub fn tokenizers(&self) -> &HashMap<String, TokenizerSpec> {
        &self.tokenizers
    }

    // Filter helpers

    pub fn insert_filter(&mut self, name: String, filter: FilterSpec) -> Option<FilterSpec> {
        self.filters.insert(name, filter)
    }

    pub fn filters(&self) -> &HashMap<String, FilterSpec> {
        &self.filters
    }

    // Analyzer helpers

    pub fn insert_analyzer(&mut self, name: String, analyzer: AnalyzerSpec) -> Option<AnalyzerSpec> {
        self.analyzers.insert(name, analyzer)
    }

    pub fn analyzers(&self) -> &HashMap<String, AnalyzerSpec> {
        &self.analyzers
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

    // Mapping helpers

    pub fn get_field_mapping(&self, name: &str) -> Option<&FieldMapping> {
        for mapping in self.mappings.values() {
            if let Some(property) = mapping.properties.get(name) {
                if let MappingProperty::Field(ref field_mapping) = *property {
                    return Some(field_mapping);
                }
            }
        }

        None
    }
}


impl Serialize for IndexMetadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Tokenizers
        let mut tokenizers_json = BTreeMap::new();
        for (name, tokenizer) in self.tokenizers.iter() {
            tokenizers_json.insert(name.to_string(), serde_json::to_value(&tokenizer).unwrap());
        }

        // Filters
        let mut filters_json = BTreeMap::new();
        for (name, filter) in self.filters.iter() {
            filters_json.insert(name.to_string(), serde_json::to_value(&filter).unwrap());
        }

        // Mappings
        let mut mappings_json = BTreeMap::new();
        for (name, mapping) in self.mappings.iter() {
            mappings_json.insert(name.to_string(), serde_json::to_value(&mapping).unwrap());
        }

        let json = json!({
            "settings": {
                "analysis": {
                    "tokenizers": tokenizers_json,
                    "filters": filters_json,
                    "analyzers": {},  // TODO
                },
            },
            "mappings": mappings_json,
        });

        json.serialize(serializer)
    }
}
