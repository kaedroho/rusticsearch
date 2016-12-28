use analysis::registry::AnalyzerRegistry;


#[derive(Debug)]
pub struct IndexSettings {
    pub analyzers: AnalyzerRegistry,
}


impl Default for  IndexSettings {
    fn default() -> IndexSettings {
        IndexSettings {
            analyzers: AnalyzerRegistry::new(),
        }
    }
}
