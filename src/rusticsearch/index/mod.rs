pub mod maintenance;
pub mod metadata;

use std::sync::RwLock;
use std::path::PathBuf;

use kite_rocksdb::RocksDBStore;
use uuid::Uuid;

use index::metadata::IndexMetadata;


#[derive(Debug)]
pub struct Index {
    id: Uuid,
    canonical_name: String,
    pub metadata: RwLock<IndexMetadata>,
    pub store: RocksDBStore,
}


impl Index {
    pub fn new(id: Uuid, canonical_name: String, metadata: IndexMetadata, store: RocksDBStore) -> Index {
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

    pub fn metadata_path(&self) -> PathBuf {
        let mut path = self.store.path().to_path_buf();
        path.push("metadata.json");
        path
    }
}
