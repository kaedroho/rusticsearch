pub mod standard;
pub mod ngram;

use token::Token;
use analysis::ngram_generator::Edge;
use analysis::tokenizers::standard::StandardTokenizer;
use analysis::tokenizers::ngram::NGramTokenizer;


/// Defines a tokenizer
///
/// You can use this to define a tokenizer before having to bind it to any data
///
/// # Examples
///
/// ```
/// use abra::{Term, Token};
/// use abra::analysis::tokenizers::TokenizerSpec;
///
/// let standard_tokenizer = TokenizerSpec::Standard;
/// let token_stream = standard_tokenizer.initialise("Hello, world!");
///
/// let tokens = token_stream.collect::<Vec<Token>>();
///
/// assert_eq!(tokens, vec![
///     Token { term: Term::String("Hello".to_string()), position: 1 },
///     Token { term: Term::String("world".to_string()), position: 2 },
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq)]
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
