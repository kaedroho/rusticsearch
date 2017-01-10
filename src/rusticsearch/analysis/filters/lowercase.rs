//! Converts each token into lowercase

use std::str;

use kite::{Term, Token};


pub struct LowercaseFilter<'a> {
    tokens: Box<Iterator<Item=Token> + 'a>,
}


impl<'a> LowercaseFilter<'a> {
    pub fn new(tokens: Box<Iterator<Item=Token> +'a>) -> LowercaseFilter<'a> {
        LowercaseFilter {
            tokens: tokens,
        }
    }
}


impl<'a> Iterator for LowercaseFilter<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        match self.tokens.next() {
            Some(token) => {
                Some(Token {
                    term: match str::from_utf8(&token.term.to_bytes()) {
                        Ok(string) => {
                            // TODO: Can this be done in place?
                            Term::from_string(&string.to_lowercase())
                        }
                        _ => token.term,
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

    use super::LowercaseFilter;

    #[test]
    fn test_lowercase_filter() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::from_string("Hulk"), position: 1 },
            Token { term: Term::from_string("SMASH"), position: 2 }
        ];

        let token_filter = LowercaseFilter::new(Box::new(tokens.drain((..))));
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::from_string("hulk"), position: 1 },
            Token { term: Term::from_string("smash"), position: 2 }
        ]);
    }

    #[test]
    fn test_lowercase_filter_cjk() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::from_string("こんにちは"), position: 1 },
            Token { term: Term::from_string("ハチ公"), position: 2 },
            Token { term: Term::from_string("Test"), position: 3 }
        ];

        let token_filter = LowercaseFilter::new(Box::new(tokens.drain((..))));
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::from_string("こんにちは"), position: 1 },
            Token { term: Term::from_string("ハチ公"), position: 2 },
            Token { term: Term::from_string("test"), position: 3 }
        ]);
    }
}
