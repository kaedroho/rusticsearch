//! The analysis module
//!
//! This module provides a library of tools for breaking down a string of text
//! into Tokens.
//!
//! These tools are sorted into three categories:
//!
//!  - Tokenisers split a string of text into a stream of tokens
//!  - Filters apply transformations to streams of tokens
//!  - Analyzers are a combination of a tokeniser and a group of filters

pub mod ngram_generator;
pub mod lucene_asciifold;
pub mod tokenizers;
pub mod filters;

use token::Token;

use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;
use analysis::ngram_generator::Edge;


/// Defines an analyzer
///
/// You can use this to define an analyzer before having to bind it to any data
///
/// # Examples
///
/// ```
/// use kite::{Term, Token};
/// use kite::analysis::tokenizers::TokenizerSpec;
/// use kite::analysis::filters::FilterSpec;
/// use kite::analysis::AnalyzerSpec;
///
/// // Define an analyzer that splits words and converts them into lowercase
/// let analyzer = AnalyzerSpec {
///     tokenizer: TokenizerSpec::Standard,
///     filters: vec![
///         FilterSpec::Lowercase,
///     ]
/// };
///
/// let token_stream = analyzer.initialise("Hello, WORLD!");
/// let tokens = token_stream.collect::<Vec<Token>>();
///
/// assert_eq!(tokens, vec![
///     Token { term: Term::String("hello".to_string()), position: 1 },
///     Token { term: Term::String("world".to_string()), position: 2 },
/// ]);
/// ```
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
