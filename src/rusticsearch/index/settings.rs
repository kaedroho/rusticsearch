use std::collections::HashMap;

use rustc_serialize::json::Json;

use analysis::ngram_generator::Edge;
use analysis::AnalyzerSpec;
use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;


#[derive(Debug)]
pub struct IndexSettings {
    pub tokenizers: HashMap<String, TokenizerSpec>,
    pub filters: HashMap<String, FilterSpec>,
    pub analyzers: HashMap<String, AnalyzerSpec>,
}


impl IndexSettings {
    pub fn new() -> IndexSettings {
        IndexSettings {
            tokenizers: HashMap::new(),
            filters: HashMap::new(),
            analyzers: HashMap::new(),
        }
    }
}


#[derive(Debug, PartialEq)]
pub enum IndexSettingsParseError {
    SomethingWentWrong,
    UnrecognisedTokenizerType(String),
    UnrecognisedFilterType(String),
}


fn parse_tokenizer(data: &Json) -> Result<TokenizerSpec, IndexSettingsParseError> {
    let data = match data.as_object() {
        Some(object) => object,
        None => return Err(IndexSettingsParseError::SomethingWentWrong),
    };

    let tokenizer_type = match data.get("type") {
        Some(type_json) => {
            match type_json.as_string() {
                Some(tokenizer_type) => tokenizer_type,
                None => return Err(IndexSettingsParseError::SomethingWentWrong),
            }
        }
        None => return Err(IndexSettingsParseError::SomethingWentWrong),
    };

    match tokenizer_type {
        "standard" => {
            Ok(TokenizerSpec::Standard)
        }
        "nGram" | "ngram" => {
            let min_gram = match data.get("min_gram") {
                Some(min_gram_json) => {
                    match min_gram_json.as_u64() {
                        Some(min_gram) => min_gram as usize,
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
                    }
                }
                None => 1 as usize,
            };

            let max_gram = match data.get("max_gram") {
                Some(max_gram_json) => {
                    match max_gram_json.as_u64() {
                        Some(max_gram) => max_gram as usize,
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
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
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
                    }
                }
                None => 1 as usize,
            };

            let max_gram = match data.get("max_gram") {
                Some(max_gram_json) => {
                    match max_gram_json.as_u64() {
                        Some(max_gram) => max_gram as usize,
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
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
                                _ => return Err(IndexSettingsParseError::SomethingWentWrong)
                            }
                        },
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
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
        _ => Err(IndexSettingsParseError::UnrecognisedTokenizerType(tokenizer_type.to_owned())),
    }
}


fn parse_filter(data: &Json) -> Result<FilterSpec, IndexSettingsParseError> {
    let data = match data.as_object() {
        Some(object) => object,
        None => return Err(IndexSettingsParseError::SomethingWentWrong),
    };

    let filter_type = match data.get("type") {
        Some(type_json) => {
            match type_json.as_string() {
                Some(filter_type) => filter_type,
                None => return Err(IndexSettingsParseError::SomethingWentWrong),
            }
        }
        None => return Err(IndexSettingsParseError::SomethingWentWrong),
    };

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
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
                    }
                }
                None => 1 as usize,
            };

            let max_gram = match data.get("max_gram") {
                Some(max_gram_json) => {
                    match max_gram_json.as_u64() {
                        Some(max_gram) => max_gram as usize,
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
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
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
                    }
                }
                None => 1 as usize,
            };

            let max_gram = match data.get("max_gram") {
                Some(max_gram_json) => {
                    match max_gram_json.as_u64() {
                        Some(max_gram) => max_gram as usize,
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
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
                                _ => return Err(IndexSettingsParseError::SomethingWentWrong)
                            }
                        },
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
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
        _ => Err(IndexSettingsParseError::UnrecognisedFilterType(filter_type.to_owned())),
    }
}


fn parse_analyzer(data: &Json, tokenizers: &HashMap<String, TokenizerSpec>, filters: &HashMap<String, FilterSpec>) -> Result<AnalyzerSpec, IndexSettingsParseError> {
    let data = match data.as_object() {
        Some(object) => object,
        None => return Err(IndexSettingsParseError::SomethingWentWrong),
    };

    let analyzer_type = match data.get("type") {
        Some(type_json) => {
            match type_json.as_string() {
                Some(analyzer_type) => analyzer_type,
                None => return Err(IndexSettingsParseError::SomethingWentWrong),
            }
        }
        None => return Err(IndexSettingsParseError::SomethingWentWrong),
    };

    match analyzer_type {
        "custom" => {
            // Get tokenizer
            let tokenizer_name = match data.get("tokenizer") {
                Some(tokenizer_json) => {
                    match tokenizer_json.as_string() {
                        Some(tokenizer) => tokenizer,
                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
                    }
                }
                None => return Err(IndexSettingsParseError::SomethingWentWrong),
            };

            let tokenizer_spec = match tokenizers.get(tokenizer_name) {
                Some(tokenizer_spec) => tokenizer_spec,
                None => return Err(IndexSettingsParseError::SomethingWentWrong),
            };

            // Build analyzer
            let mut analyzer_spec = AnalyzerSpec {
                tokenizer: tokenizer_spec.clone(),
                filters: Vec::new(),
            };

            // Add filters
            if let Some(filter_json) = data.get("filter") {
                match filter_json.as_array() {
                    Some(filter_names) => {
                        for filter_name_json in filter_names.iter() {
                            // Get filter
                            match filter_name_json.as_string() {
                                Some(filter_name) => {
                                    let filter_spec = match filters.get(filter_name) {
                                        Some(filter_spec) => filter_spec,
                                        None => return Err(IndexSettingsParseError::SomethingWentWrong),
                                    };

                                    analyzer_spec.filters.push(filter_spec.clone());
                                }
                                None => return Err(IndexSettingsParseError::SomethingWentWrong),
                            }
                        }
                    },
                    None => return Err(IndexSettingsParseError::SomethingWentWrong),
                }
            }

            Ok(analyzer_spec)
        }
        // TODO
        // default/standard
        // standard_html_strip
        // simple
        // stop
        // whitespace
        // keyword
        // pattern
        // snowball
        // arabic
        // armenian
        // basque
        // brazilian
        // bulgarian
        // catalan
        // chinese
        // cjk
        // czech
        // danish
        // dutch
        // english
        // finnish
        // french
        // galician
        // german
        // greek
        // hindi
        // hungarian
        // indonesian
        // irish
        // italian
        // latvian
        // lithuanian
        // norwegian
        // persian
        // portuguese
        // romanian
        // sorani
        // spanish
        // swedish
        // turkish
        // thai
        // fingerprint
        _ => Err(IndexSettingsParseError::SomethingWentWrong),
    }
}


pub fn parse(data: Json) -> Result<IndexSettings, IndexSettingsParseError> {
    let data = match data.as_object() {
        Some(object) => object,
        None => {
            return Err(IndexSettingsParseError::SomethingWentWrong);
        }
    };

    let mut index_settings = IndexSettings::new();

    if let Some(settings) = data.get("settings") {
        let settings = match settings.as_object() {
            Some(object) => object,
            None => return Err(IndexSettingsParseError::SomethingWentWrong),
        };

        if let Some(analysis) = settings.get("analysis") {
            let analysis = match analysis.as_object() {
                Some(object) => object,
                None => return Err(IndexSettingsParseError::SomethingWentWrong),
            };

            // Tokenisers
            if let Some(tokenizer_data) = analysis.get("tokenizer") {
                let tokenizer_data = match tokenizer_data.as_object() {
                    Some(object) => object,
                    None => return Err(IndexSettingsParseError::SomethingWentWrong),
                };

                for (name, data) in tokenizer_data {
                    let tokenizer = try!(parse_tokenizer(data));
                    index_settings.tokenizers.insert(name.clone(), tokenizer);
                }
            }

            // Token filters
            if let Some(filter_data) = analysis.get("filter") {
                let filter_data = match filter_data.as_object() {
                    Some(object) => object,
                    None => return Err(IndexSettingsParseError::SomethingWentWrong),
                };

                for (name, data) in filter_data {
                    let filter = try!(parse_filter(data));
                    index_settings.filters.insert(name.clone(), filter);
                }
            }

            // Analyzers
            if let Some(analyzer_data) = analysis.get("analyzer") {
                let analyzer_data = match analyzer_data.as_object() {
                    Some(object) => object,
                    None => return Err(IndexSettingsParseError::SomethingWentWrong),
                };

                for (name, data) in analyzer_data {
                    let analyzer = try!(parse_analyzer(data, &index_settings.tokenizers, &index_settings.filters));
                    index_settings.analyzers.insert(name.clone(), analyzer);
                }
            }
        }
    }


    Ok(index_settings)
}


#[cfg(test)]
mod tests {
    use rustc_serialize::json::Json;

    use abra::analysis::ngram_generator::Edge;
    use abra::analysis::tokenizers::TokenizerSpec;
    use abra::analysis::filters::FilterSpec;

    use super::{parse, IndexSettingsParseError};

    #[test]
    fn test_empty() {
        let settings = parse(Json::from_str("
        {
        }
        ").unwrap());

        let settings = settings.expect("parse() returned an error");

        assert_eq!(settings.tokenizers.len(), 0);
        assert_eq!(settings.filters.len(), 0);
        assert_eq!(settings.analyzers.len(), 0);
    }

    #[test]
    fn test_custom_analyser() {
        let settings = parse(Json::from_str("
        {
            \"settings\": {
                \"analysis\": {
                    \"tokenizer\": {
                        \"ngram_tokenizer\": {
                            \"type\": \"nGram\",
                            \"min_gram\": 3,
                            \"max_gram\": 15
                        },
                        \"edgengram_tokenizer\": {
                            \"type\": \"edgeNGram\",
                            \"min_gram\": 2,
                            \"max_gram\": 15
                        },
                        \"edgengram_tokenizer_side_front\": {
                            \"type\": \"edgeNGram\",
                            \"min_gram\": 2,
                            \"max_gram\": 15,
                            \"side\": \"front\"
                        },
                        \"edgengram_tokenizer_side_back\": {
                            \"type\": \"edgeNGram\",
                            \"min_gram\": 2,
                            \"max_gram\": 15,
                            \"side\": \"back\"
                        }
                    },
                    \"filter\": {
                        \"ngram_filter\": {
                            \"type\": \"nGram\",
                            \"min_gram\": 3,
                            \"max_gram\": 15
                        },
                        \"edgengram_filter\": {
                            \"type\": \"edgeNGram\",
                            \"min_gram\": 2,
                            \"max_gram\": 15
                        },
                        \"edgengram_filter_side_front\": {
                            \"type\": \"edgeNGram\",
                            \"min_gram\": 2,
                            \"max_gram\": 15,
                            \"side\": \"front\"
                        },
                        \"edgengram_filter_side_back\": {
                            \"type\": \"edgeNGram\",
                            \"min_gram\": 2,
                            \"max_gram\": 15,
                            \"side\": \"back\"
                        }
                    }
                }
            }
        }
        ").unwrap());

        let settings = settings.expect("parse() returned an error");

        assert_eq!(settings.tokenizers.len(), 4);
        assert_eq!(settings.filters.len(), 4);
        assert_eq!(settings.analyzers.len(), 0);

        // Check tokenizers
        let ngram_tokenizer = settings.tokenizers.get("ngram_tokenizer").expect("'ngram_tokenizer' wasn't created");
        assert_eq!(*ngram_tokenizer, TokenizerSpec::NGram {
            min_size: 3,
            max_size: 15,
            edge: Edge::Neither,
        });

        let edgengram_tokenizer = settings.tokenizers.get("edgengram_tokenizer").expect("'edgengram_tokenizer' wasn't created");
        assert_eq!(*edgengram_tokenizer, TokenizerSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Left,
        });

        let edgengram_tokenizer_side_front = settings.tokenizers.get("edgengram_tokenizer_side_front").expect("'edgengram_tokenizer_side_front' wasn't created");
        assert_eq!(*edgengram_tokenizer_side_front, TokenizerSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Left,
        });

        let edgengram_tokenizer_side_back = settings.tokenizers.get("edgengram_tokenizer_side_back").expect("'edgengram_tokenizer_side_back' wasn't created");
        assert_eq!(*edgengram_tokenizer_side_back, TokenizerSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Right,
        });

        // Check filters
        let ngram_filter = settings.filters.get("ngram_filter").expect("'ngram_filter' wasn't created");
        assert_eq!(*ngram_filter, FilterSpec::NGram {
            min_size: 3,
            max_size: 15,
            edge: Edge::Neither,
        });

        let edgengram_filter = settings.filters.get("edgengram_filter").expect("'edgengram_filter' wasn't created");
        assert_eq!(*edgengram_filter, FilterSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Left,
        });

        let edgengram_filter_side_front = settings.filters.get("edgengram_filter_side_front").expect("'edgengram_filter_side_front' wasn't created");
        assert_eq!(*edgengram_filter_side_front, FilterSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Left,
        });

        let edgengram_filter_side_back = settings.filters.get("edgengram_filter_side_back").expect("'edgengram_filter_side_back' wasn't created");
        assert_eq!(*edgengram_filter_side_back, FilterSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Right,
        });
    }

    #[test]
    fn test_custom_analyser_bad_tokenizer_type() {
        let settings = parse(Json::from_str("
        {
            \"settings\": {
                \"analysis\": {
                    \"tokenizer\": {
                        \"bad_tokenizer\": {
                            \"type\": \"foo\"
                        }
                    }
                }
            }
        }
        ").unwrap());

        let error = settings.err().expect("parse() was supposed to return an error, but didn't");

        assert_eq!(error, IndexSettingsParseError::UnrecognisedTokenizerType("foo".to_string()))
    }

    #[test]
    fn test_custom_analyser_bad_filter_type() {
        let settings = parse(Json::from_str("
        {
            \"settings\": {
                \"analysis\": {
                    \"filter\": {
                        \"bad_filter\": {
                            \"type\": \"foo\"
                        }
                    }
                }
            }
        }
        ").unwrap());

        let error = settings.err().expect("parse() was supposed to return an error, but didn't");

        assert_eq!(error, IndexSettingsParseError::UnrecognisedFilterType("foo".to_string()))
    }
}
