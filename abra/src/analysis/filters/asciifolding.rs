use term::Term;
use token::Token;
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
                    term: match token.term {
                        Term::String(ref string) => {
                            Term::String(fold_to_ascii(string))
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

    use super::ASCIIFoldingFilter;

    #[test]
    fn test_simple() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::String("Ĥéllø".to_string()), position: 1 },
        ];

        let token_filter = ASCIIFoldingFilter::new(Box::new(tokens.drain((..))));
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("Hello".to_string()), position: 1 }
        ]);
    }

    #[test]
    fn test_hiragana_not_changed() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::String("こんにちは".to_string()), position: 1 },
            Token { term: Term::String("ハチ公".to_string()), position: 2 },
        ];

        let token_filter = ASCIIFoldingFilter::new(Box::new(tokens.drain((..))));
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("こんにちは".to_string()), position: 1 },
            Token { term: Term::String("ハチ公".to_string()), position: 2 },
        ]);
    }
}
