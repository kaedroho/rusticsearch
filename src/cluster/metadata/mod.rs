pub mod name_registry;

use std::collections::HashMap;

use uuid::Uuid;

use index::Index;

use self::name_registry::NameRegistry;


#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct IndexRef(Uuid);


impl IndexRef {
    pub fn id(&self) -> &Uuid {
        &self.0
    }
}


#[derive(Debug)]
pub struct ClusterMetadata {
    pub indices: HashMap<IndexRef, Index>,
    pub names: NameRegistry,
}


impl ClusterMetadata {
    pub fn new() -> ClusterMetadata {
        ClusterMetadata {
            indices: HashMap::new(),
            names: NameRegistry::new(),
        }
    }

    pub fn insert_index(&mut self, index: Index) -> IndexRef {
        let index_ref = IndexRef(index.id().clone());
        self.indices.insert(index_ref, index);

        index_ref
    }
}
