use rustc_serialize::json::Json;

use analysis::ngram_generator::Edge;
use analysis::tokenizers::TokenizerSpec;


#[derive(Debug, PartialEq)]
pub enum TokenizerParseError {
    ExpectedObject,
    ExpectedString,
    ExpectedPositiveInteger,
    ExpectedKey(String),
    UnrecognisedType(String),
    InvalidSideValue,
}


pub fn parse(json: &Json) -> Result<TokenizerSpec, TokenizerParseError> {
    let data = try!(json.as_object().ok_or(TokenizerParseError::ExpectedObject));

    // Get type
    let tokenizer_type_json = try!(data.get("type").ok_or(TokenizerParseError::ExpectedKey("type".to_string())));
    let tokenizer_type = try!(tokenizer_type_json.as_string().ok_or(TokenizerParseError::ExpectedString));

    match tokenizer_type {
        "standard" => {
            Ok(TokenizerSpec::Standard)
        }
        "nGram" | "ngram" => {
            let min_gram = match data.get("min_gram") {
                Some(min_gram_json) => {
                    match min_gram_json.as_u64() {
                        Some(min_gram) => min_gram as usize,
                        None => return Err(TokenizerParseError::ExpectedPositiveInteger),
                    }
                }
                None => 1 as usize,
            };

            let max_gram = match data.get("max_gram") {
                Some(max_gram_json) => {
                    match max_gram_json.as_u64() {
                        Some(max_gram) => max_gram as usize,
                        None => return Err(TokenizerParseError::ExpectedPositiveInteger),
                    }
                }
                None => 2 as usize,
            };

            Ok(TokenizerSpec::NGram {
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
                        None => return Err(TokenizerParseError::ExpectedPositiveInteger),
                    }
                }
                None => 1 as usize,
            };

            let max_gram = match data.get("max_gram") {
                Some(max_gram_json) => {
                    match max_gram_json.as_u64() {
                        Some(max_gram) => max_gram as usize,
                        None => return Err(TokenizerParseError::ExpectedPositiveInteger),
                    }
                }
                None => 2 as usize,
            };

            let edge = match data.get("side") {
                Some(side_json) => {
                    match side_json.as_string() {
                        Some(side_string) => {
                            let side_string_lower = side_string.to_lowercase();
                            match side_string_lower.as_ref() {
                                "front" => {
                                    Edge::Left
                                }
                                "back" => {
                                    Edge::Right
                                }
                                _ => return Err(TokenizerParseError::InvalidSideValue)
                            }
                        },
                        None => return Err(TokenizerParseError::ExpectedString),
                    }
                }
                None => Edge::Left,
            };

            Ok(TokenizerSpec::NGram {
                min_size: min_gram,
                max_size: max_gram,
                edge: edge,
            })
        }
        // TODO
        // uax_url_email
        // path_hierarchy/PathHierarchy
        // keyword
        // letter
        // lowercase
        // whitespace
        // pattern
        // classic
        // thai
        _ => Err(TokenizerParseError::UnrecognisedType(tokenizer_type.to_owned())),
    }
}
