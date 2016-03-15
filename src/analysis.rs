use std::cmp;

use unidecode::unidecode;
use unicode_segmentation::UnicodeSegmentation;


pub enum AnalyzerStep {
    ToLowercase,
    ToUppercase,
    LimitLength{min: usize, max: Option<usize>},
    MakeNGrams{min_gram: usize, max_gram: Option<usize>},
    MakeEdgeNGrams{min_gram: usize, max_gram: Option<usize>},
    ASCIIFold,
    SplitUnicodeWords,
}

#[derive(Debug)]
enum AnalyzerStepResult {
    Some(String),
    Multiple(Vec<String>),
    None,
}

impl AnalyzerStep {
    pub fn run(&self, token: String) -> AnalyzerStepResult {
        match *self {
            AnalyzerStep::ToLowercase => AnalyzerStepResult::Some(token.to_lowercase()),
            AnalyzerStep::ToUppercase => AnalyzerStepResult::Some(token.to_uppercase()),
            AnalyzerStep::LimitLength{min, max} => {
                let len = token.len();

                if len < min {
                    return AnalyzerStepResult::None;
                }

                if let Some(max) = max {
                    if len > max {
                        return AnalyzerStepResult::None
                    }
                }

                AnalyzerStepResult::Some(token)
            }
            AnalyzerStep::MakeNGrams{min_gram, max_gram} => {
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

                AnalyzerStepResult::Multiple(ngrams)
            }
            AnalyzerStep::MakeEdgeNGrams{min_gram, max_gram} => {
                let mut ngrams = Vec::new();

                let max_gram = match max_gram {
                    Some(max_gram) => cmp::min(max_gram, token.len()),
                    None => token.len(),
                };
                for last_char in (0 + min_gram)..(0 + max_gram + 1) {
                    ngrams.push(token[0..last_char].to_string());
                }

                AnalyzerStepResult::Multiple(ngrams)
            }
            AnalyzerStep::ASCIIFold => AnalyzerStepResult::Some(unidecode(&token)),
            AnalyzerStep::SplitUnicodeWords => {
                AnalyzerStepResult::Multiple(
                    token.unicode_words()
                         .map(|s| s.to_string())
                         .collect()
                )
            }
        }
    }
}


pub struct Analyzer {
    pub steps: Vec<AnalyzerStep>,
}

impl Analyzer {
    fn run_step(&self, step: &AnalyzerStep, tokens: Vec<String>) -> Vec<String> {
        let mut new_tokens = Vec::new();

        for token in tokens {
            match step.run(token) {
                AnalyzerStepResult::Some(s) => new_tokens.push(s),
                AnalyzerStepResult::None => {},
                AnalyzerStepResult::Multiple(v) => new_tokens.extend_from_slice(&v),
            }
        }

        new_tokens
    }

    pub fn analyze(&self, tokens: Vec<String>) -> Vec<String> {
        let mut tokens = tokens;

        for step in self.steps.iter() {
            tokens = self.run_step(step, tokens);
        }

        tokens
    }
}

// TODO fn parse_analyzer()
