pub mod maintenance;
pub mod registry;
pub mod metadata;
pub mod metadata_parser;

use kite_rocksdb::RocksDBIndexStore;

use mapping::{Mapping, FieldMapping};
use index::metadata::IndexMetaData;


#[derive(Debug)]
pub struct Index {
    name: String,
    pub metadata: IndexMetaData,
    pub store: RocksDBIndexStore,
}


impl Index {
    pub fn new(name: String, metadata: IndexMetaData, store: RocksDBIndexStore) -> Index {
        Index {
            name: name,
            metadata: metadata,
            store: store,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn get_mapping_by_name(&self, name: &str) -> Option<&Mapping> {
        self.metadata.mappings.get(name)
    }

    pub fn get_field_mapping_by_name(&self, name: &str) -> Option<&FieldMapping> {
        self.metadata.mappings.get_field(name)
    }
}
