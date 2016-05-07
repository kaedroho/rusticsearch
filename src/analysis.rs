use std::cmp;

use unidecode::unidecode;
use unicode_segmentation::UnicodeSegmentation;


#[derive(Debug, PartialEq)]
pub enum Analyzer {
    None,
    Standard,
    EdgeNGram,
}


impl Analyzer {
    pub fn run(&self, input: String) -> Vec<String> {
        match *self {
            Analyzer::None => vec![input],
            Analyzer::Standard => {
                // Lowercase
                let input = input.to_lowercase();

                // Convert string to ascii (not standard in Elasticsearch, but Wagtail needs it)
                let input = unidecode(&input);

                // Tokenise
                let tokens = input.unicode_words()
                                  .map(|s| s.to_string())
                                  .collect();

                tokens
            }
            Analyzer::EdgeNGram => {
                // Analyze with standard analyzer
                let tokens = Analyzer::Standard.run(input);

                // Generate ngrams
                let mut ngrams = Vec::new();
                let min_gram = 2;
                let max_gram = Some(15);

                for token in tokens.iter() {
                    let max_gram = match max_gram {
                        Some(max_gram) => cmp::min(max_gram, token.len()),
                        None => token.len(),
                    };
                    for last_char in (0 + min_gram)..(0 + max_gram + 1) {
                        // TODO: Currently breaks on non-ascii code points
                        ngrams.push(token[0..last_char].to_string());
                    }
                }

                ngrams
            }
        }
    }
}
