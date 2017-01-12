pub mod parse;

use analysis::registry::AnalyzerRegistry;
use mapping::MappingRegistry;


#[derive(Debug)]
pub struct IndexMetaData {
    pub analyzers: AnalyzerRegistry,
    pub mappings: MappingRegistry,
}


impl Default for  IndexMetaData {
    fn default() -> IndexMetaData {
        IndexMetaData {
            analyzers: AnalyzerRegistry::new(),
            mappings: MappingRegistry::new(),
        }
    }
}
