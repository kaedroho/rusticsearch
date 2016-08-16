use std::collections::VecDeque;

use term::Term;
use token::Token;
use analysis::ngram_generator::{Edge, NGramGenerator};


pub struct NGramFilter<'a> {
    tokens: Box<Iterator<Item=Token> + 'a>,
    min_size: usize,
    max_size: usize,
    edge: Edge,
    output_buffer: VecDeque<Token>,
}


impl<'a> NGramFilter<'a> {
    pub fn new(tokens: Box<Iterator<Item=Token> +'a >, min_size: usize, max_size: usize, edge: Edge) -> NGramFilter<'a> {
        NGramFilter {
            tokens: tokens,
            min_size: min_size,
            max_size: max_size,
            edge: edge,
            output_buffer: VecDeque::new(),
        }
    }
}


impl<'a> Iterator for NGramFilter<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        while self.output_buffer.is_empty() {
            // Generate ngrams for next token
            let token = self.tokens.next();

            match token {
                Some(token) => {
                    if let Term::String(ref word) = token.term {
                        let mut ngram_generator = NGramGenerator::new(&word, self.min_size, self.max_size, self.edge);

                        for gram in ngram_generator {
                            self.output_buffer.push_back(Token {
                                term: Term::String(gram.to_string()),
                                position: token.position,
                            });
                        }
                    }
                }
                None => return None
            }
        }

        self.output_buffer.pop_front()
    }
}


#[cfg(test)]
mod tests {
    use term::Term;
    use token::Token;
    use analysis::ngram_generator::Edge;

    use super::NGramFilter;

    #[test]
    fn test_ngram_filter() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::String("hello".to_string()), position: 1 },
        ];

        let token_filter = NGramFilter::new(Box::new(tokens.drain((..))), 2, 3, Edge::Neither);
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("he".to_string()), position: 1 },
            Token { term: Term::String("hel".to_string()), position: 1 },
            Token { term: Term::String("el".to_string()), position: 1 },
            Token { term: Term::String("ell".to_string()), position: 1 },
            Token { term: Term::String("ll".to_string()), position: 1 },
            Token { term: Term::String("llo".to_string()), position: 1 },
            Token { term: Term::String("lo".to_string()), position: 1 },
        ]);
    }

    #[test]
    fn test_edgengram_filter() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::String("hello".to_string()), position: 1 },
            Token { term: Term::String("world".to_string()), position: 2 }
        ];

        let token_filter = NGramFilter::new(Box::new(tokens.drain((..))), 2, 3, Edge::Left);
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("he".to_string()), position: 1 },
            Token { term: Term::String("hel".to_string()), position: 1 },
            Token { term: Term::String("wo".to_string()), position: 2 },
            Token { term: Term::String("wor".to_string()), position: 2 },
        ]);
    }

    #[test]
    fn test_edgengram_filter_max_size() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::String("hello".to_string()), position: 1 },
        ];

        let token_filter = NGramFilter::new(Box::new(tokens.drain((..))), 2, 1000, Edge::Left);
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("he".to_string()), position: 1 },
            Token { term: Term::String("hel".to_string()), position: 1 },
            Token { term: Term::String("hell".to_string()), position: 1 },
            Token { term: Term::String("hello".to_string()), position: 1 },
        ]);
    }

    #[test]
    fn test_edgengram_filter_right() {
        let mut tokens: Vec<Token> = vec![
            Token { term: Term::String("hello".to_string()), position: 1 },
            Token { term: Term::String("world".to_string()), position: 2 }
        ];

        let token_filter = NGramFilter::new(Box::new(tokens.drain((..))), 2, 3, Edge::Right);
        let tokens = token_filter.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("lo".to_string()), position: 1 },
            Token { term: Term::String("llo".to_string()), position: 1 },
            Token { term: Term::String("ld".to_string()), position: 2 },
            Token { term: Term::String("rld".to_string()), position: 2 },
        ]);
    }
}
