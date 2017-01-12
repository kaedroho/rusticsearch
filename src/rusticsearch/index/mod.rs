pub mod maintenance;
pub mod registry;
pub mod metadata;

use std::sync::RwLock;

use kite_rocksdb::RocksDBIndexStore;
use uuid::Uuid;

use index::metadata::IndexMetaData;


#[derive(Debug)]
pub struct Index {
    id: Uuid,
    canonical_name: String,
    pub metadata: RwLock<IndexMetaData>,
    pub store: RocksDBIndexStore,
}


impl Index {
    pub fn new(id: Uuid, canonical_name: String, metadata: IndexMetaData, store: RocksDBIndexStore) -> Index {
        Index {
            id: id,
            canonical_name: canonical_name,
            metadata: RwLock::new(metadata),
            store: store,
        }
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn canonical_name(&self) -> &str {
        &self.canonical_name
    }
}
