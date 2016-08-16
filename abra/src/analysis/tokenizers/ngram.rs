use unicode_segmentation::{UnicodeSegmentation, UnicodeWords};

use term::Term;
use token::Token;
use analysis::ngram_generator::{Edge, NGramGenerator};


pub struct NGramTokenizer<'a> {
    unicode_words: UnicodeWords<'a>,
    min_size: usize,
    max_size: usize,
    edge: Edge,
    position_counter: u32,
    ngram_generator: Option<NGramGenerator<'a>>,
}


impl<'a> NGramTokenizer<'a> {
    pub fn new(input: &'a str, min_size: usize, max_size: usize, edge: Edge) -> NGramTokenizer<'a> {
        NGramTokenizer {
            unicode_words: input.unicode_words(),
            min_size: min_size,
            max_size: max_size,
            edge: edge,
            position_counter: 0,
            ngram_generator: None
        }
    }
}


impl<'a> Iterator for NGramTokenizer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        loop {
            // Get next ngram
            if let Some(ref mut ngram_generator) = self.ngram_generator {
                if let Some(gram) = ngram_generator.next() {
                    return Some(Token {
                        term: Term::String(gram.to_string()),
                        position: self.position_counter,
                    });
                }
            }

            // No more ngrams for this word, get next word
            let word = self.unicode_words.next();

            match word {
                Some(word) => {
                    self.position_counter += 1;
                    self.ngram_generator = Some(
                        NGramGenerator::new(word, self.min_size, self.max_size, self.edge)
                    );
                }
                None => return None,
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use term::Term;
    use token::Token;
    use analysis::ngram_generator::Edge;

    use super::NGramTokenizer;


    #[test]
    fn test_ngram_tokenizer() {
        let tokenizer = NGramTokenizer::new("hello", 2, 3, Edge::Neither);
        let tokens = tokenizer.collect::<Vec<Token>>();

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
    fn test_edgengram_tokenizer() {
        let tokenizer = NGramTokenizer::new("hello world", 2, 3, Edge::Left);
        let tokens = tokenizer.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("he".to_string()), position: 1 },
            Token { term: Term::String("hel".to_string()), position: 1 },
            Token { term: Term::String("wo".to_string()), position: 2 },
            Token { term: Term::String("wor".to_string()), position: 2 },
        ]);
    }

    #[test]
    fn test_edgengram_tokenizer_max_size() {
        let tokenizer = NGramTokenizer::new("hello", 2, 1000, Edge::Left);
        let tokens = tokenizer.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("he".to_string()), position: 1 },
            Token { term: Term::String("hel".to_string()), position: 1 },
            Token { term: Term::String("hell".to_string()), position: 1 },
            Token { term: Term::String("hello".to_string()), position: 1 },
        ]);
    }

    #[test]
    fn test_edgengram_tokenizer_right() {
        let tokenizer = NGramTokenizer::new("hello world", 2, 3, Edge::Right);
        let tokens = tokenizer.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("lo".to_string()), position: 1 },
            Token { term: Term::String("llo".to_string()), position: 1 },
            Token { term: Term::String("ld".to_string()), position: 2 },
            Token { term: Term::String("rld".to_string()), position: 2 },
        ]);
    }
}
