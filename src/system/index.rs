use std::collections::HashSet;

use search::analysis::registry::AnalyzerRegistry;
use search::mapping::{Mapping, FieldMapping, MappingRegistry};
use search::store::memory::MemoryIndexStore;


#[derive(Debug)]
pub struct Index {
    pub analyzers: AnalyzerRegistry,
    pub mappings: MappingRegistry,
    pub aliases: HashSet<String>,
    pub store: MemoryIndexStore,
}


impl Index {
    pub fn new(store: MemoryIndexStore) -> Index {
        Index {
            analyzers: AnalyzerRegistry::new(),
            mappings: MappingRegistry::new(),
            aliases: HashSet::new(),
            store: store,
        }
    }

    pub fn get_mapping_by_name(&self, name: &str) -> Option<&Mapping> {
        self.mappings.get(name)
    }

    pub fn get_field_mapping_by_name(&self, name: &str) -> Option<&FieldMapping> {
        self.mappings.get_field(name)
    }
}
