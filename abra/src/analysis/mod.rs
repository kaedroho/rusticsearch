pub mod ngram_generator;
pub mod lucene_asciifold;
pub mod registry;
pub mod tokenizers;
pub mod filters;

use term::Term;
use token::Token;

use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;
use analysis::ngram_generator::Edge;
use analysis::tokenizers::standard::StandardTokenizer;
use analysis::filters::lowercase::LowercaseFilter;
use analysis::filters::asciifolding::ASCIIFoldingFilter;
use analysis::filters::ngram::NGramFilter;


#[derive(Debug)]
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


#[derive(Debug, PartialEq)]
pub enum Analyzer {
    Standard,
    EdgeNGram,
}


impl Analyzer {
    pub fn run(&self, input: String) -> Vec<Token> {
        match *self {
            Analyzer::Standard => {
                let tokens = Box::new(StandardTokenizer::new(&input));

                // Lowercase
                let tokens = Box::new(LowercaseFilter::new(tokens));

                // ASCII Folding (not standard in Elasticsearch, but Wagtail needs it)
                let tokens = Box::new(ASCIIFoldingFilter::new(tokens));

                tokens.collect::<Vec<Token>>()
            }
            Analyzer::EdgeNGram => {
                let tokens = Box::new(StandardTokenizer::new(&input));

                // Lowercase
                let tokens = Box::new(LowercaseFilter::new(tokens));

                // ASCII Folding (not standard in Elasticsearch, but Wagtail needs it)
                let tokens = Box::new(ASCIIFoldingFilter::new(tokens));

                // Ngrams
                let tokens = Box::new(NGramFilter::new(tokens, 2, 15, Edge::Left));

                tokens.collect::<Vec<Token>>()
            }
        }
    }
}
