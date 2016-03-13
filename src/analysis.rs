use std::cmp;

use unidecode::unidecode;


pub enum Tokenizer {
    Standard{max_token_length: usize},
    Letter,
    Whitespace,
    NGram{
        min_gram: usize,
        max_gram: usize,
        token_chars__letter: bool,
        token_chars__digit: bool,
        token_chars__whitespace: bool,
        token_chars__punctuation: bool,
        token_chars__symbol: bool,
    },
    EdgeNGram{
        min_gram: usize,
        max_gram: usize,
        token_chars__letter: bool,
        token_chars__digit: bool,
        token_chars__whitespace: bool,
        token_chars__punctuation: bool,
        token_chars__symbol: bool,
    },

}

impl Tokenizer {
    pub fn tokenize(&self, input: String) -> Vec<String> {
        // TODO
        Vec::new()
    }
}


pub enum TokenFilter {
    Standard,
    ASCIIFolding,
    Length{min: usize, max: Option<usize>},
    Lowercase,
    Uppercase,
    NGram{min_gram: usize, max_gram: Option<usize>},
    EdgeNGram{min_gram: usize, max_gram: Option<usize>},
}

#[derive(Debug)]
enum TokenFilterResult {
    Some(String),
    Multiple(Vec<String>),
    None,
}

impl TokenFilter {
    pub fn filter(&self, token: String) -> TokenFilterResult {
        match *self {
            TokenFilter::Standard => TokenFilterResult::Some(token),
            TokenFilter::ASCIIFolding => TokenFilterResult::Some(unidecode(&token)),
            TokenFilter::Length{min, max} => {
                let len = token.len();

                if len < min {
                    return TokenFilterResult::None;
                }

                if let Some(max) = max {
                    if len > max {
                        return TokenFilterResult::None
                    }
                }

                TokenFilterResult::Some(token)
            }
            TokenFilter::Lowercase => TokenFilterResult::Some(token.to_lowercase()),
            TokenFilter::Uppercase => TokenFilterResult::Some(token.to_uppercase()),
            TokenFilter::NGram{min_gram, max_gram} => {
                let mut ngrams = Vec::new();

                for first_char in 0..token.len() {
                    let max_gram = match max_gram {
                        Some(max_gram) => cmp::min(max_gram, token.len() - first_char),
                        None => token.len() - first_char,
                    };
                    for last_char in (first_char + min_gram)..(first_char + max_gram + 1) {
                        ngrams.push(token[first_char..last_char].to_string());
                    }
                }

                TokenFilterResult::Multiple(ngrams)
            }
            TokenFilter::EdgeNGram{min_gram, max_gram} => {
                let mut ngrams = Vec::new();

                let max_gram = match max_gram {
                    Some(max_gram) => cmp::min(max_gram, token.len()),
                    None => token.len(),
                };
                for last_char in (0 + min_gram)..(0 + max_gram + 1) {
                    ngrams.push(token[0..last_char].to_string());
                }

                TokenFilterResult::Multiple(ngrams)
            }
        }
    }
}


struct Analyzer {
    tokenizer: Tokenizer,
    token_filters: Vec<TokenFilter>,
}

// TODO fn parse_analyzer()
