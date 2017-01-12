pub mod analysis;
pub mod mapping;
pub mod parse;

use index::metadata::analysis::AnalyzerRegistry;
use index::metadata::mapping::MappingRegistry;


#[derive(Debug)]
pub struct IndexMetaData {
    pub analyzers: AnalyzerRegistry,
    pub mappings: MappingRegistry,
}


impl Default for IndexMetaData {
    fn default() -> IndexMetaData {
        IndexMetaData {
            analyzers: AnalyzerRegistry::new(),
            mappings: MappingRegistry::new(),
        }
    }
}
