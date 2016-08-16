use unicode_segmentation::{UnicodeSegmentation, UnicodeWords};

use term::Term;
use token::Token;


pub struct StandardTokenizer<'a> {
    input: &'a str,
    unicode_words: UnicodeWords<'a>,
    position_counter: u32,
}


impl<'a> StandardTokenizer<'a> {
    pub fn new(input: &'a str) -> StandardTokenizer<'a> {
        StandardTokenizer {
            input: input,
            unicode_words: input.unicode_words(),
            position_counter: 0,
        }
    }
}


impl<'a> Iterator for StandardTokenizer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        match self.unicode_words.next() {
            Some(mut word) => {
                self.position_counter += 1;

                Some(Token {
                    term: Term::String(word.to_string()),
                    position: self.position_counter,
                })
            }
            None => None,
        }

    }
}


#[cfg(test)]
mod tests {
    use term::Term;
    use token::Token;

    use super::StandardTokenizer;

    const TEXT: &'static str = "Up from the bowels of hell he sails, weilding a tankard of freshly brewed ale!";

    #[test]
    fn test_standard_tokenizer() {
        let tokenizer = StandardTokenizer::new(TEXT);
        let tokens = tokenizer.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("Up".to_string()), position: 1 },
            Token { term: Term::String("from".to_string()), position: 2 },
            Token { term: Term::String("the".to_string()), position: 3 },
            Token { term: Term::String("bowels".to_string()), position: 4 },
            Token { term: Term::String("of".to_string()), position: 5 },
            Token { term: Term::String("hell".to_string()), position: 6 },
            Token { term: Term::String("he".to_string()), position: 7 },
            Token { term: Term::String("sails".to_string()), position: 8 },
            Token { term: Term::String("weilding".to_string()), position: 9 },
            Token { term: Term::String("a".to_string()), position: 10 },
            Token { term: Term::String("tankard".to_string()), position: 11 },
            Token { term: Term::String("of".to_string()), position: 12 },
            Token { term: Term::String("freshly".to_string()), position: 13 },
            Token { term: Term::String("brewed".to_string()), position: 14 },
            Token { term: Term::String("ale".to_string()), position: 15 }
        ]);
    }

    #[test]
    fn test_standard_tokenizer_cjk() {
        let tokenizer = StandardTokenizer::new("こんにちは、ハチ公！");
        let tokens = tokenizer.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::String("こ".to_string()), position: 1 },
            Token { term: Term::String("ん".to_string()), position: 2 },
            Token { term: Term::String("に".to_string()), position: 3 },
            Token { term: Term::String("ち".to_string()), position: 4 },
            Token { term: Term::String("は".to_string()), position: 5 },
            Token { term: Term::String("ハチ".to_string()), position: 6 },
            Token { term: Term::String("公".to_string()), position: 7 },
        ]);
    }
}
