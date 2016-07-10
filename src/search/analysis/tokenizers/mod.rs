pub mod standard;

use search::token::Token;
use search::analysis::tokenizers::standard::StandardTokenizer;


#[derive(Debug)]
pub enum TokenizerSpec {
    Standard,
}


impl TokenizerSpec {
    pub fn initialise<'a>(&self, input: &'a str) -> Box<Iterator<Item=Token> + 'a> {
        match *self {
            TokenizerSpec::Standard => {
                Box::new(StandardTokenizer::new(input))
            }
        }
    }
}
