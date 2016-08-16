use std::cmp;

use unicode_segmentation::UnicodeSegmentation;


#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Edge {
    Neither,
    Left,
    Right,
}


#[derive(Debug)]
pub struct NGramGenerator<'a> {
    word: &'a str,
    word_len: usize,
    min_size: usize,
    max_size: usize,
    edge: Edge,

    current_position: usize,
    current_size: usize,
    finished: bool,
}


impl <'a> NGramGenerator<'a> {
    pub fn new(word: &'a str, min_size: usize, max_size: usize, edge: Edge) -> NGramGenerator {
        let word_len = word.graphemes(true).count();
        let max_size = cmp::min(max_size, word_len);

        NGramGenerator {
            word: word,
            word_len: word_len,
            min_size: min_size,
            max_size: max_size,
            edge: edge,

            current_position: 0,
            current_size: min_size,
            finished: (max_size == 0 || min_size > word_len),
        }
    }

    /// Retrieve the current gram
    #[inline]
    fn current_gram(&self) -> &'a str {
        let mut start = self.current_position;

        // On right edge we take from the end of the string, instead of the beginning
        if self.edge == Edge::Right {
            start += self.word_len - self.current_position - self.current_size;
        }

        // Find byte positions of the first and last graphemes we are interested in
        let mut grapheme_indices = self.word.grapheme_indices(true).skip(start).take(self.current_size);

        let first_grapheme = match grapheme_indices.next() {
            Some(first_grapheme) => first_grapheme,
            None => return "",
        };

        let last_grapheme = match grapheme_indices.last() {
            Some(last_grapheme) => last_grapheme,
            None => first_grapheme,
        };

        // Slice the original string using the byte positions of first/last grapheme
        let first_byte = first_grapheme.0;
        let last_byte = last_grapheme.0 + last_grapheme.1.len();
        &self.word[first_byte..last_byte]
    }

    #[inline]
    fn current_max_size(&self) -> usize {
        cmp::min(self.max_size, self.word_len - self.current_position)
    }

    /// Advance to the next gram
    /// Note: this will set the "finished" attribute if there are no grams left
    fn next_gram(&mut self) {
        self.current_size += 1;

        if self.current_size > self.current_max_size() {
            if self.edge != Edge::Neither {
                self.finished = true;
                return;
            }

            self.current_size = self.min_size;
            self.current_position += 1;

            if self.current_size > self.current_max_size() {
                self.finished = true;
                return;
            }
        }
    }
}


impl <'a> Iterator for NGramGenerator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        if self.finished {
            return None;
        }

        let gram = self.current_gram();
        self.next_gram();
        Some(gram)
    }
}


#[cfg(test)]
mod tests {
    use term::Term;
    use token::Token;

    use super::{Edge, NGramGenerator};

    #[test]
    fn test_ngram() {
        let gen = NGramGenerator::new("hello", 2, 3, Edge::Neither);
        let ngrams = gen.collect::<Vec<&str>>();

        assert_eq!(ngrams, vec![
            "he", "hel", "el", "ell", "ll", "llo", "lo"
        ]);
    }

    #[test]
    fn test_ngram_left_edge() {
        let gen = NGramGenerator::new("hello", 2, 4, Edge::Left);
        let ngrams = gen.collect::<Vec<&str>>();

        assert_eq!(ngrams, vec![
            "he", "hel", "hell"
        ]);
    }

    #[test]
    fn test_ngram_right_edge() {
        let gen = NGramGenerator::new("hello", 2, 4, Edge::Right);
        let ngrams = gen.collect::<Vec<&str>>();

        assert_eq!(ngrams, vec![
            "lo", "llo", "ello"
        ]);
    }

    #[test]
    fn test_ngram_cjk() {
        let gen = NGramGenerator::new("こんにちは", 2, 3, Edge::Neither);
        let ngrams = gen.collect::<Vec<&str>>();

        assert_eq!(ngrams, vec![
            "こん", "こんに", "んに", "んにち", "にち", "にちは", "ちは"
        ]);
    }

    #[test]
    fn test_ngram_graphemes() {
        let gen = NGramGenerator::new("u͔n͈̰̎i̙̮͚̦c͚̉o̼̩̰͗d͔̆̓ͥé", 2, 3, Edge::Neither);
        let ngrams = gen.collect::<Vec<&str>>();

        assert_eq!(ngrams, vec![
            "u\u{354}n\u{30e}\u{348}\u{330}",
            "u\u{354}n\u{30e}\u{348}\u{330}i\u{319}\u{32e}\u{35a}\u{326}",
            "n\u{30e}\u{348}\u{330}i\u{319}\u{32e}\u{35a}\u{326}",
            "n\u{30e}\u{348}\u{330}i\u{319}\u{32e}\u{35a}\u{326}c\u{309}\u{35a}",
            "i\u{319}\u{32e}\u{35a}\u{326}c\u{309}\u{35a}",
            "i\u{319}\u{32e}\u{35a}\u{326}c\u{309}\u{35a}o\u{357}\u{33c}\u{329}\u{330}",
            "c\u{309}\u{35a}o\u{357}\u{33c}\u{329}\u{330}",
            "c\u{309}\u{35a}o\u{357}\u{33c}\u{329}\u{330}d\u{306}\u{343}\u{365}\u{354}",
            "o\u{357}\u{33c}\u{329}\u{330}d\u{306}\u{343}\u{365}\u{354}",
            "o\u{357}\u{33c}\u{329}\u{330}d\u{306}\u{343}\u{365}\u{354}e\u{301}",
            "d\u{306}\u{343}\u{365}\u{354}e\u{301}",
        ]);
    }

    #[test]
    fn test_ngram_blank_string() {
        let gen = NGramGenerator::new("", 2, 3, Edge::Neither);
        let ngrams = gen.collect::<Vec<&str>>();

        let empty_result: Vec<&str> = vec![];
        assert_eq!(ngrams, empty_result);
    }

    #[test]
    fn test_ngram_high_size() {
        let gen = NGramGenerator::new("hello", 20, 20, Edge::Neither);
        let ngrams = gen.collect::<Vec<&str>>();

        let empty_result: Vec<&str> = vec![];
        assert_eq!(ngrams, empty_result);
    }

    #[test]
    fn test_ngram_zero_size() {
        let gen = NGramGenerator::new("hello", 0, 0, Edge::Neither);
        let ngrams = gen.collect::<Vec<&str>>();

        let empty_result: Vec<&str> = vec![];
        assert_eq!(ngrams, empty_result);
    }

    #[test]
    fn test_ngram_invalid_size() {
        // TODO: Should this panic?
        let gen = NGramGenerator::new("hello", 20, 5, Edge::Neither);
        let ngrams = gen.collect::<Vec<&str>>();

        let empty_result: Vec<&str> = vec![];
        assert_eq!(ngrams, empty_result);
    }
}
