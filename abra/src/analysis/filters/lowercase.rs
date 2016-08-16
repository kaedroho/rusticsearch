use term::Term;
use token::Token;


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
                    term: match token.term {
                        Term::String(string) => {
                            // TODO: Can this be done in place?
                            Term::String(string.to_lowercase())
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
    use term::Term;
    use token::Token;

    use super::LowercaseFilter;

    #[test]
    fn test_lowercase_filter() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::String("Hulk".to_string()), position: 1 },
            Token { term: Term::String("SMASH".to_string()), position: 2 }
        ];

        let token_filter = LowercaseFilter::new(Box::new(tokens.drain((..))));
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("hulk".to_string()), position: 1 },
            Token { term: Term::String("smash".to_string()), position: 2 }
        ]);
    }

    #[test]
    fn test_lowercase_filter_cjk() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::String("こんにちは".to_string()), position: 1 },
            Token { term: Term::String("ハチ公".to_string()), position: 2 },
            Token { term: Term::String("Test".to_string()), position: 3 }
        ];

        let token_filter = LowercaseFilter::new(Box::new(tokens.drain((..))));
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("こんにちは".to_string()), position: 1 },
            Token { term: Term::String("ハチ公".to_string()), position: 2 },
            Token { term: Term::String("test".to_string()), position: 3 }
        ]);
    }
}
