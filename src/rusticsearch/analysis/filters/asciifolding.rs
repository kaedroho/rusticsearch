//! Converts any non-ASCII character into ASCII if a reasonable equivilent exists
//!
//! For example, "Ĥéllø" is converted to "Hello" but non-latin scripts such as
//! arabic or hiragana are not changed.

use std::str;

use kite::{Term, Token};

use analysis::lucene_asciifold::fold_to_ascii;


pub struct ASCIIFoldingFilter<'a> {
    tokens: Box<Iterator<Item=Token> + 'a>,
}


impl<'a> ASCIIFoldingFilter<'a> {
    pub fn new(tokens: Box<Iterator<Item=Token> +'a >) -> ASCIIFoldingFilter<'a> {
        ASCIIFoldingFilter {
            tokens: tokens,
        }
    }
}


impl<'a> Iterator for ASCIIFoldingFilter<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        match self.tokens.next() {
            Some(token) => {
                Some(Token {
                    term: match str::from_utf8(token.term.as_bytes()) {
                        Ok(ref string) => {
                            Term::from_string(&fold_to_ascii(string))
                        }
                        _ => token.term.clone(),
                    },
                    position: token.position,
                })
            }
            None => None
        }
    }
}


#[cfg(test)]
mod tests {
    use kite::{Term, Token};

    use super::ASCIIFoldingFilter;

    #[test]
    fn test_simple() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::from_string("Ĥéllø"), position: 1 },
        ];

        let token_filter = ASCIIFoldingFilter::new(Box::new(tokens.drain((..))));
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::from_string("Hello"), position: 1 }
        ]);
    }

    #[test]
    fn test_hiragana_not_changed() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::from_string("こんにちは"), position: 1 },
            Token { term: Term::from_string("ハチ公"), position: 2 },
        ];

        let token_filter = ASCIIFoldingFilter::new(Box::new(tokens.drain((..))));
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::from_string("こんにちは"), position: 1 },
            Token { term: Term::from_string("ハチ公"), position: 2 },
        ]);
    }
}
