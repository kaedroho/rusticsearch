pub mod ngram_generator;
pub mod lucene_asciifold;
pub mod registry;
pub mod tokenizers;
pub mod filters;

use std::cmp;

use unidecode::unidecode;
use unicode_segmentation::UnicodeSegmentation;

use search::term::Term;
use search::token::Token;


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
                // Lowercase
                let input = input.to_lowercase();

                // Convert string to ascii (not standard in Elasticsearch, but Wagtail needs it)
                let input = unidecode(&input);

                // Tokenise
                let mut position = 0;
                let tokens = input.unicode_words()
                                  .map(|s| {
                                      position += 1;

                                      Token {
                                          term: Term::String(s.to_string()),
                                          position: position,
                                      }
                                  })
                                  .collect();

                tokens
            }
            Analyzer::EdgeNGram => {
                // Analyze with standard analyzer
                let tokens = Analyzer::Standard.run(input);

                // Generate ngrams
                let mut ngrams = Vec::new();
                let min_gram = 2;
                let max_gram = Some(15);

                for token in tokens {
                    if let Term::String(s) = token.term {
                        let max_gram = match max_gram {
                            Some(max_gram) => cmp::min(max_gram, s.len()),
                            None => s.len(),
                        };
                        for last_char in (0 + min_gram)..(0 + max_gram + 1) {
                            // TODO: Currently breaks on non-ascii code points
                            ngrams.push(Token {
                                term: Term::String(s[0..last_char].to_string()),
                                position: token.position,
                            });
                        }
                    }
                }

                ngrams
            }
        }
    }
}
