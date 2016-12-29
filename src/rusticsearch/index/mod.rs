pub mod maintenance;
pub mod registry;
pub mod metadata;
pub mod metadata_parser;

use kite_rocksdb::RocksDBIndexStore;

use mapping::{Mapping, FieldMapping, MappingRegistry};
use index::metadata::IndexMetaData;


#[derive(Debug)]
pub struct Index {
    name: String,
    pub metadata: IndexMetaData,
    pub mappings: MappingRegistry,
    pub store: RocksDBIndexStore,
}


impl Index {
    pub fn new(name: String, metadata: IndexMetaData, store: RocksDBIndexStore) -> Index {
        Index {
            name: name,
            metadata: metadata,
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
