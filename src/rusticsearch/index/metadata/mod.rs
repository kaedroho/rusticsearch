pub mod analysis;
pub mod parse;

use std::collections::HashMap;

use mapping::{Mapping, FieldMapping};
use index::metadata::analysis::AnalyzerRegistry;


#[derive(Debug)]
pub struct IndexMetaData {
    pub analyzers: AnalyzerRegistry,
    pub mappings: HashMap<String, Mapping>,
}


impl Default for IndexMetaData {
    fn default() -> IndexMetaData {
        IndexMetaData {
            analyzers: AnalyzerRegistry::new(),
            mappings: HashMap::new(),
        }
    }
}


impl IndexMetaData {
    pub fn get_field_mapping(&self, name: &str) -> Option<&FieldMapping> {
        for mapping in self.mappings.values() {
            if let Some(ref field_mapping) = mapping.fields.get(name) {
                return Some(field_mapping);
            }
        }

        None
    }
}
