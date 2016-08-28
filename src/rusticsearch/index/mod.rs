pub mod maintenance;
pub mod registry;
pub mod settings;

use kite_rocksdb::RocksDBIndexStore;

use analysis::registry::AnalyzerRegistry;
use mapping::{Mapping, FieldMapping, MappingRegistry};


#[derive(Debug)]
pub struct Index {
    name: String,
    pub analyzers: AnalyzerRegistry,
    pub mappings: MappingRegistry,
    pub store: RocksDBIndexStore,
}


impl Index {
    pub fn new(name: String, store: RocksDBIndexStore) -> Index {
        Index {
            name: name,
            analyzers: AnalyzerRegistry::new(),
            mappings: MappingRegistry::new(),
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
