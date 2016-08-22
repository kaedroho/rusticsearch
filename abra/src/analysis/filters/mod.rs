pub mod lowercase;
pub mod ngram;
pub mod asciifolding;

use token::Token;
use analysis::ngram_generator::Edge;
use analysis::filters::lowercase::LowercaseFilter;
use analysis::filters::ngram::NGramFilter;
use analysis::filters::asciifolding::ASCIIFoldingFilter;


#[derive(Debug, Clone)]
pub enum FilterSpec {
    Lowercase,
    NGram {
        min_size: usize,
        max_size: usize,
        edge: Edge,
    },
    ASCIIFolding,
}


impl FilterSpec {
    pub fn initialise<'a>(&self, input: Box<Iterator<Item=Token> + 'a>) -> Box<Iterator<Item=Token> + 'a> {
        match *self {
            FilterSpec::Lowercase => {
                Box::new(LowercaseFilter::new(input))
            }
            FilterSpec::NGram{min_size, max_size, edge} => {
                Box::new(NGramFilter::new(input, min_size, max_size, edge))
            }
            FilterSpec::ASCIIFolding => {
                Box::new(ASCIIFoldingFilter::new(input))
            }
        }
    }
}
