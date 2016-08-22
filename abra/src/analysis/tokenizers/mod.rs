pub mod standard;
pub mod ngram;

use token::Token;
use analysis::ngram_generator::Edge;
use analysis::tokenizers::standard::StandardTokenizer;
use analysis::tokenizers::ngram::NGramTokenizer;


#[derive(Debug, Clone)]
pub enum TokenizerSpec {
    Standard,
    NGram {
        min_size: usize,
        max_size: usize,
        edge: Edge,
    }
}


impl TokenizerSpec {
    pub fn initialise<'a>(&self, input: &'a str) -> Box<Iterator<Item=Token> + 'a> {
        match *self {
            TokenizerSpec::Standard => {
                Box::new(StandardTokenizer::new(input))
            }
            TokenizerSpec::NGram{min_size, max_size, edge} => {
                Box::new(NGramTokenizer::new(input, min_size, max_size, edge))
            }
        }
    }
}
