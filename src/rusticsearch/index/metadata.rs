use analysis::registry::AnalyzerRegistry;


#[derive(Debug)]
pub struct IndexMetaData {
    pub analyzers: AnalyzerRegistry,
}


impl Default for  IndexMetaData {
    fn default() -> IndexMetaData {
        IndexMetaData {
            analyzers: AnalyzerRegistry::new(),
        }
    }
}
