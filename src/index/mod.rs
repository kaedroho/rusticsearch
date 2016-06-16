pub mod store;

use std::collections::{BTreeMap, HashMap, HashSet};

use term::Term;
use mapping::{Mapping, FieldMapping, MappingRegistry};
use document::Document;
use index::store::memory::MemoryIndexStore;


#[derive(Debug)]
pub struct Index {
    pub mappings: MappingRegistry,
    pub aliases: HashSet<String>,
    pub backend: MemoryIndexStore,
}


impl Index {
    pub fn new() -> Index {
        Index {
            mappings: MappingRegistry::new(),
            aliases: HashSet::new(),
            backend: MemoryIndexStore::new(),
        }
    }

    pub fn get_mapping_by_name(&self, name: &str) -> Option<&Mapping> {
        self.mappings.get(name)
    }

    pub fn get_field_mapping_by_name(&self, name: &str) -> Option<&FieldMapping> {
        self.mappings.get_field(name)
    }

    pub fn initialise(&mut self) {}
}
