pub mod maintenance;
pub mod registry;
pub mod metadata;
pub mod metadata_parser;

use kite_rocksdb::RocksDBIndexStore;
use uuid::Uuid;

use mapping::{Mapping, FieldMapping};
use index::metadata::IndexMetaData;


#[derive(Debug)]
pub struct Index {
    id: Uuid,
    canonical_name: String,
    pub metadata: IndexMetaData,
    pub store: RocksDBIndexStore,
}


impl Index {
    pub fn new(id: Uuid, canonical_name: String, metadata: IndexMetaData, store: RocksDBIndexStore) -> Index {
        Index {
            id: id,
            canonical_name: canonical_name,
            metadata: metadata,
            store: store,
        }
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn canonical_name(&self) -> &str {
        &self.canonical_name
    }

    pub fn get_mapping_by_name(&self, name: &str) -> Option<&Mapping> {
        self.metadata.mappings.get(name)
    }

    pub fn get_field_mapping_by_name(&self, name: &str) -> Option<&FieldMapping> {
        self.metadata.mappings.get_field(name)
    }
}
