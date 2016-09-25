pub mod registry;

use std::collections::HashSet;

use abra::store::memory::MemoryIndexStore;

use analysis::registry::AnalyzerRegistry;
use mapping::{Mapping, FieldMapping, MappingRegistry};


#[derive(Debug)]
pub struct Index {
    name: String,
    pub analyzers: AnalyzerRegistry,
    pub mappings: MappingRegistry,
    pub aliases: HashSet<String>,
    pub store: MemoryIndexStore,
}


impl Index {
    pub fn new(name: String, store: MemoryIndexStore) -> Index {
        Index {
            name: name,
            analyzers: AnalyzerRegistry::new(),
            mappings: MappingRegistry::new(),
            aliases: HashSet::new(),
            store: store,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn get_mapping_by_name(&self, name: &str) -> Option<&Mapping> {
        self.mappings.get(name)
    }

    pub fn get_field_mapping_by_name(&self, name: &str) -> Option<&FieldMapping> {
        self.mappings.get_field(name)
    }
}
