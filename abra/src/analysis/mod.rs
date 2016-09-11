pub mod ngram_generator;
pub mod lucene_asciifold;
pub mod registry;
pub mod tokenizers;
pub mod filters;

use token::Token;

use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;
use analysis::ngram_generator::Edge;


#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzerSpec {
    pub tokenizer: TokenizerSpec,
    pub filters: Vec<FilterSpec>,
}


impl AnalyzerSpec {
    pub fn initialise<'a>(&self, input: &'a str) -> Box<Iterator<Item=Token> + 'a> {
        let mut analyzer = self.tokenizer.initialise(input);

        for filter in self.filters.iter() {
            analyzer = filter.initialise(analyzer);
        }

        analyzer
    }
}
