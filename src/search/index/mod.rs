pub mod reader;
pub mod store;

use std::collections::{BTreeMap, HashMap, HashSet};

use search::term::Term;
use search::analysis::registry::AnalyzerRegistry;
use search::mapping::{Mapping, FieldMapping, MappingRegistry};
use search::document::Document;
use search::index::store::rocksdb::RocksDBIndexStore;


#[derive(Debug)]
pub struct Index {
    pub analyzers: AnalyzerRegistry,
    pub mappings: MappingRegistry,
    pub aliases: HashSet<String>,
    pub store: RocksDBIndexStore,
}


impl Index {
    pub fn new(store: RocksDBIndexStore) -> Index {
        Index {
            analyzers: AnalyzerRegistry::new(),
            mappings: MappingRegistry::new(),
            aliases: HashSet::new(),
            store: store,
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
