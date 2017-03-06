use serde_json;

use analysis::ngram_generator::Edge;
use analysis::filters::FilterSpec;


#[derive(Debug, PartialEq)]
pub enum FilterParseError {
    ExpectedObject,
    ExpectedString,
    ExpectedPositiveInteger,
    ExpectedKey(String),
    UnrecognisedType(String),
    InvalidSideValue,
}


pub fn parse(json: &serde_json::Value) -> Result<FilterSpec, FilterParseError> {
    let data = try!(json.as_object().ok_or(FilterParseError::ExpectedObject));

    // Get type
    let filter_type_json = try!(data.get("type").ok_or(FilterParseError::ExpectedKey("type".to_string())));
    let filter_type = try!(filter_type_json.as_str().ok_or(FilterParseError::ExpectedString));

    match filter_type {
        "asciifolding" => {
            Ok(FilterSpec::ASCIIFolding)
        }
        "lowercase" => {
            Ok(FilterSpec::Lowercase)
        }
        "nGram" | "ngram" => {
            let min_gram = match data.get("min_gram") {
                Some(min_gram_json) => {
                    match min_gram_json.as_u64() {
                        Some(min_gram) => min_gram as usize,
                        None => return Err(FilterParseError::ExpectedPositiveInteger),
                    }
                }
                None => 1 as usize,
            };

            let max_gram = match data.get("max_gram") {
                Some(max_gram_json) => {
                    match max_gram_json.as_u64() {
                        Some(max_gram) => max_gram as usize,
                        None => return Err(FilterParseError::ExpectedPositiveInteger),
                    }
                }
                None => 2 as usize,
            };

            Ok(FilterSpec::NGram {
                min_size: min_gram,
                max_size: max_gram,
                edge: Edge::Neither,
            })
        }
        "edgeNGram" | "edge_ngram" => {
            let min_gram = match data.get("min_gram") {
                Some(min_gram_json) => {
                    match min_gram_json.as_u64() {
                        Some(min_gram) => min_gram as usize,
                        None => return Err(FilterParseError::ExpectedPositiveInteger),
                    }
                }
                None => 1 as usize,
            };

            let max_gram = match data.get("max_gram") {
                Some(max_gram_json) => {
                    match max_gram_json.as_u64() {
                        Some(max_gram) => max_gram as usize,
                        None => return Err(FilterParseError::ExpectedPositiveInteger),
                    }
                }
                None => 2 as usize,
            };

            let edge = match data.get("side") {
                Some(side_json) => {
                    match side_json.as_str() {
                        Some(side_string) => {
                            let side_string_lower = side_string.to_lowercase();
                            match side_string_lower.as_ref() {
                                "front" => {
                                    Edge::Left
                                }
                                "back" => {
                                    Edge::Right
                                }
                                _ => return Err(FilterParseError::InvalidSideValue)
                            }
                        },
                        None => return Err(FilterParseError::ExpectedString),
                    }
                }
                None => Edge::Left,
            };

            Ok(FilterSpec::NGram {
                min_size: min_gram,
                max_size: max_gram,
                edge: edge,
            })
        }
        // TODO
        // stop
        // reverse
        // length
        // uppercase
        // porter_stem
        // kstem
        // standard
        // shingle
        // unique
        // truncate
        // trim
        // limit
        // common_grams
        // snowball
        // stemmer
        // word_delimiter
        // delimited_payload_filter
        // elision
        // keep
        // keep_types
        // pattern_capture
        // pattern_replace
        // dictionary_decompounder
        // hyphenation_decompounder
        // arabic_stem
        // brazilian_stem
        // czech_stem
        // dutch_stem
        // french_stem
        // german_stem
        // russian_stem
        // keyword_marker
        // stemmer_override
        // arabic_normalization
        // german_normalization
        // hindi_normalization
        // indic_normalization
        // sorani_normalization
        // persian_normalization
        // scandinavian_normalization
        // scandinavian_folding
        // serbian_normalization
        // hunspell
        // cjk_bigram
        // cjk_width
        // apostrophe
        // classic
        // decimal_digit
        // fingerprint
        _ => Err(FilterParseError::UnrecognisedType(filter_type.to_string())),
    }
}
