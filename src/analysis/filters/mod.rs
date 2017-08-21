pub mod lowercase;
pub mod ngram;
pub mod asciifolding;

use serde::{Serialize, Serializer};
use kite::Token;

use analysis::ngram_generator::Edge;
use analysis::filters::lowercase::LowercaseFilter;
use analysis::filters::ngram::NGramFilter;
use analysis::filters::asciifolding::ASCIIFoldingFilter;


/// Defines a token filter
///
/// You can use this to define a token filter before having to bind it to any data
///
/// # Examples
///
/// ```
/// use kite::{Term, Token};
/// use kite::analysis::tokenizers::TokenizerSpec;
/// use kite::analysis::filters::FilterSpec;
///
/// let standard_tokenizer = TokenizerSpec::Standard;
/// let token_stream = standard_tokenizer.initialise("Hello, WORLD!");
///
/// // Lowercase filter
/// let lowercase_filter = FilterSpec::Lowercase;
/// let filtered_token_stream = lowercase_filter.initialise(token_stream);
///
/// let tokens = filtered_token_stream.collect::<Vec<Token>>();
///
/// assert_eq!(tokens, vec![
///     Token { term: Term::from_string("hello"), position: 1 },
///     Token { term: Term::from_string("world"), position: 2 },
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq)]
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


impl Serialize for FilterSpec {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let json = match *self {
            FilterSpec::Lowercase => {
                json!({
                    "type": "lowercase",
                })
            }
            FilterSpec::NGram{min_size, max_size, edge} => {
                match edge {
                    Edge::Left => {
                        json!({
                            "type": "edgeNGram",
                            "side": "front",
                            "min_gram": min_size,
                            "max_gram": max_size,
                        })
                    }
                    Edge::Right => {
                        json!({
                            "type": "edgeNGram",
                            "side": "back",
                            "min_gram": min_size,
                            "max_gram": max_size,
                        })
                    }
                    Edge::Neither => {
                        json!({
                            "type": "ngram",
                            "min_gram": min_size,
                            "max_gram": max_size,
                        })
                    }
                }
            }
            FilterSpec::ASCIIFolding => {
                json!({
                    "type": "asciifolding",
                })
            }
        };

        json.serialize(serializer)
    }
}
