//! Splits strings by word boundaries, according to the Unicode Standard [Annex #29](http://unicode.org/reports/tr29/) rules

use unicode_segmentation::{UnicodeSegmentation, UnicodeWords};

use kite::{Term, Token};


pub struct StandardTokenizer<'a> {
    unicode_words: UnicodeWords<'a>,
    position_counter: u32,
}


impl<'a> StandardTokenizer<'a> {
    pub fn new(input: &'a str) -> StandardTokenizer<'a> {
        StandardTokenizer {
            unicode_words: input.unicode_words(),
            position_counter: 0,
        }
    }
}


impl<'a> Iterator for StandardTokenizer<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        match self.unicode_words.next() {
            Some(word) => {
                self.position_counter += 1;

                Some(Token {
                    term: Term::from_string(word.to_string()),
                    position: self.position_counter,
                })
            }
            None => None,
        }

    }
}


#[cfg(test)]
mod tests {
    use kite::{Term, Token};

    use super::StandardTokenizer;

    const TEXT: &'static str = "Up from the bowels of hell he sails, weilding a tankard of freshly brewed ale!";

    #[test]
    fn test_standard_tokenizer() {
        let tokenizer = StandardTokenizer::new(TEXT);
        let tokens = tokenizer.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::from_string("Up".to_string()), position: 1 },
            Token { term: Term::from_string("from".to_string()), position: 2 },
            Token { term: Term::from_string("the".to_string()), position: 3 },
            Token { term: Term::from_string("bowels".to_string()), position: 4 },
            Token { term: Term::from_string("of".to_string()), position: 5 },
            Token { term: Term::from_string("hell".to_string()), position: 6 },
            Token { term: Term::from_string("he".to_string()), position: 7 },
            Token { term: Term::from_string("sails".to_string()), position: 8 },
            Token { term: Term::from_string("weilding".to_string()), position: 9 },
            Token { term: Term::from_string("a".to_string()), position: 10 },
            Token { term: Term::from_string("tankard".to_string()), position: 11 },
            Token { term: Term::from_string("of".to_string()), position: 12 },
            Token { term: Term::from_string("freshly".to_string()), position: 13 },
            Token { term: Term::from_string("brewed".to_string()), position: 14 },
            Token { term: Term::from_string("ale".to_string()), position: 15 }
        ]);
    }

    #[test]
    fn test_standard_tokenizer_cjk() {
        let tokenizer = StandardTokenizer::new("こんにちは、ハチ公！");
        let tokens = tokenizer.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::from_string("こ".to_string()), position: 1 },
            Token { term: Term::from_string("ん".to_string()), position: 2 },
            Token { term: Term::from_string("に".to_string()), position: 3 },
            Token { term: Term::from_string("ち".to_string()), position: 4 },
            Token { term: Term::from_string("は".to_string()), position: 5 },
            Token { term: Term::from_string("ハチ".to_string()), position: 6 },
            Token { term: Term::from_string("公".to_string()), position: 7 },
        ]);
    }
}
