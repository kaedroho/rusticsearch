use std::collections::HashMap;

use rustc_serialize::json::Json;

use analysis::ngram_generator::Edge;
use analysis::AnalyzerSpec;
use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;


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


pub enum IndexSettingsParseError {
    SomethingWentWrong,
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
        "uax_url_email" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "path_hierarchy" | "PathHierarchy" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "keyword" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "letter" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "lowercase" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "whitespace" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
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
        "pattern" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "classic" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "thai" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        _ => Err(IndexSettingsParseError::SomethingWentWrong),
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
        "stop" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "reverse" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "asciifolding" => {
            Ok(FilterSpec::ASCIIFolding)
        }
        "length" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "lowercase" => {
            Ok(FilterSpec::Lowercase)
        }
        "uppercase" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "porter_stem" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "kstem" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "standard" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
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
        "shingle" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "unique" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "truncate" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "trim" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "limit" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "common_grams" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "snowball" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "stemmer" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "word_delimiter" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "delimited_payload_filter" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "elision" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "keep" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "keep_types" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "pattern_capture" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "pattern_replace" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "dictionary_decompounder" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "hyphenation_decompounder" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "arabic_stem" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "brazilian_stem" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "czech_stem" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "dutch_stem" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "french_stem" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "german_stem" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "russian_stem" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "keyword_marker" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "stemmer_override" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "arabic_normalization" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "german_normalization" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "hindi_normalization" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "indic_normalization" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "sorani_normalization" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "persian_normalization" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "scandinavian_normalization" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "scandinavian_folding" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "serbian_normalization" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "hunspell" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "cjk_bigram" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "cjk_width" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "apostrophe" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "classic" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "decimal_digit" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "fingerprint" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        _ => Err(IndexSettingsParseError::SomethingWentWrong),
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
        "default" | "standard" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "standard_html_strip" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "simple" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "stop" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "whitespace" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "keyword" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "pattern" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "snowball" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "arabic" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "armenian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "basque" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "brazilian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "bulgarian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "catalan" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "chinese" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "cjk" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "czech" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "danish" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "dutch" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "english" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "finnish" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "french" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "galician" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "german" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "greek" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "hindi" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "hungarian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "indonesian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "irish" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "italian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "latvian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "lithuanian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "norwegian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "persian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "portuguese" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "romanian" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "sorani" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "spanish" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "swedish" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "turkish" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "thai" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
        "fingerprint" => {
            Err(IndexSettingsParseError::SomethingWentWrong)
        }
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
