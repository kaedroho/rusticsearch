pub mod ngram_generator;
pub mod lucene_asciifold;
pub mod registry;
pub mod tokenizers;
pub mod filters;

use std::cmp;

use unicode_segmentation::UnicodeSegmentation;

use search::term::Term;
use search::token::Token;

use search::analysis::ngram_generator::Edge;
use search::analysis::tokenizers::standard::StandardTokenizer;
use search::analysis::filters::lowercase::LowercaseFilter;
use search::analysis::filters::asciifolding::ASCIIFoldingFilter;
use search::analysis::filters::ngram::NGramFilter;


#[derive(Debug, PartialEq)]
pub enum Analyzer {
    None,
    Standard,
    EdgeNGram,
}


impl Analyzer {
    pub fn run(&self, input: String) -> Vec<Token> {
        match *self {
            Analyzer::None => vec![Token{term: Term::String(input), position: 1}],
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
