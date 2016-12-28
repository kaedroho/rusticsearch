pub mod maintenance;
pub mod registry;
pub mod settings;
pub mod settings_parser;

use kite_rocksdb::RocksDBIndexStore;

use mapping::{Mapping, FieldMapping, MappingRegistry};
use index::settings::IndexSettings;


#[derive(Debug)]
pub struct Index {
    name: String,
    pub settings: IndexSettings,
    pub mappings: MappingRegistry,
    pub store: RocksDBIndexStore,
}


impl Index {
    pub fn new(name: String, settings: IndexSettings, store: RocksDBIndexStore) -> Index {
        Index {
            name: name,
            settings: settings,
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
