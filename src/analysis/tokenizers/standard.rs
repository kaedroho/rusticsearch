//! Splits strings by word boundaries, according to the Unicode Standard [Annex #29](http://unicode.org/reports/tr29/) rules

use unicode_segmentation::{UnicodeSegmentation, UnicodeWords};

use search::{Term, Token};


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
                    term: Term::from_string(word),
                    position: self.position_counter,
                })
            }
            None => None,
        }

    }
}


#[cfg(test)]
mod tests {
    use search::{Term, Token};

    use super::StandardTokenizer;

    const TEXT: &'static str = "Up from the bowels of hell he sails, weilding a tankard of freshly brewed ale!";

    #[test]
    fn test_standard_tokenizer() {
        let tokenizer = StandardTokenizer::new(TEXT);
        let tokens = tokenizer.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::from_string("Up"), position: 1 },
            Token { term: Term::from_string("from"), position: 2 },
            Token { term: Term::from_string("the"), position: 3 },
            Token { term: Term::from_string("bowels"), position: 4 },
            Token { term: Term::from_string("of"), position: 5 },
            Token { term: Term::from_string("hell"), position: 6 },
            Token { term: Term::from_string("he"), position: 7 },
            Token { term: Term::from_string("sails"), position: 8 },
            Token { term: Term::from_string("weilding"), position: 9 },
            Token { term: Term::from_string("a"), position: 10 },
            Token { term: Term::from_string("tankard"), position: 11 },
            Token { term: Term::from_string("of"), position: 12 },
            Token { term: Term::from_string("freshly"), position: 13 },
            Token { term: Term::from_string("brewed"), position: 14 },
            Token { term: Term::from_string("ale"), position: 15 }
        ]);
    }

    #[test]
    fn test_standard_tokenizer_cjk() {
        let tokenizer = StandardTokenizer::new("こんにちは、ハチ公！");
        let tokens = tokenizer.collect::<Vec<Token>>();

        assert_eq!(tokens, vec![
            Token { term: Term::from_string("こ"), position: 1 },
            Token { term: Term::from_string("ん"), position: 2 },
            Token { term: Term::from_string("に"), position: 3 },
            Token { term: Term::from_string("ち"), position: 4 },
            Token { term: Term::from_string("は"), position: 5 },
            Token { term: Term::from_string("ハチ"), position: 6 },
            Token { term: Term::from_string("公"), position: 7 },
        ]);
    }
}
