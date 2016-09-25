pub mod analysis_tokenizer;
pub mod analysis_filter;
pub mod analysis_analyzer;

use rustc_serialize::json::Json;

use index::settings::IndexSettings;

use self::analysis_tokenizer::{TokenizerParseError, parse as parse_tokenizer};
use self::analysis_filter::{FilterParseError, parse as parse_filter};
use self::analysis_analyzer::{AnalyzerParseError, parse as parse_analyzer};


#[derive(Debug, PartialEq)]
pub enum IndexSettingsParseError {
    ExpectedObject,
    TokenizerParseError(String, TokenizerParseError),
    FilterParseError(String, FilterParseError),
    AnalyzerParseError(String, AnalyzerParseError),
}


pub fn parse(data: Json) -> Result<IndexSettings, IndexSettingsParseError> {
    let data = match data.as_object() {
        Some(object) => object,
        None => {
            return Err(IndexSettingsParseError::ExpectedObject);
        }
    };

    let mut index_settings = IndexSettings::new();

    if let Some(settings) = data.get("settings") {
        let settings = match settings.as_object() {
            Some(object) => object,
            None => return Err(IndexSettingsParseError::ExpectedObject),
        };

        if let Some(analysis) = settings.get("analysis") {
            let analysis = match analysis.as_object() {
                Some(object) => object,
                None => return Err(IndexSettingsParseError::ExpectedObject),
            };

            // Tokenisers
            if let Some(tokenizer_data) = analysis.get("tokenizer") {
                let tokenizer_data = match tokenizer_data.as_object() {
                    Some(object) => object,
                    None => return Err(IndexSettingsParseError::ExpectedObject),
                };

                for (name, data) in tokenizer_data {
                    let tokenizer = match parse_tokenizer(data) {
                        Ok(tokenizer) => tokenizer,
                        Err(e) => return Err(IndexSettingsParseError::TokenizerParseError(name.to_string(), e)),
                    };

                    index_settings.tokenizers.insert(name.clone(), tokenizer);
                }
            }

            // Token filters
            if let Some(filter_data) = analysis.get("filter") {
                let filter_data = match filter_data.as_object() {
                    Some(object) => object,
                    None => return Err(IndexSettingsParseError::ExpectedObject),
                };

                for (name, data) in filter_data {
                    let filter = match parse_filter(data) {
                        Ok(filter) => filter,
                        Err(e) => return Err(IndexSettingsParseError::FilterParseError(name.to_string(), e)),
                    };

                    index_settings.filters.insert(name.clone(), filter);
                }
            }

            // Analyzers
            if let Some(analyzer_data) = analysis.get("analyzer") {
                let analyzer_data = match analyzer_data.as_object() {
                    Some(object) => object,
                    None => return Err(IndexSettingsParseError::ExpectedObject),
                };

                for (name, data) in analyzer_data {
                    let analyzer = match parse_analyzer(data, &index_settings.tokenizers, &index_settings.filters) {
                        Ok(analyzer) => analyzer,
                        Err(e) => return Err(IndexSettingsParseError::AnalyzerParseError(name.to_string(), e)),
                    };

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

    use analysis::ngram_generator::Edge;
    use analysis::tokenizers::TokenizerSpec;
    use analysis::filters::FilterSpec;

    use super::{parse, IndexSettingsParseError};
    use super::analysis_tokenizer::TokenizerParseError;
    use super::analysis_filter::FilterParseError;

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

        assert_eq!(error, IndexSettingsParseError::TokenizerParseError("bad_tokenizer".to_string(), TokenizerParseError::UnrecognisedType("foo".to_string())));
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

        assert_eq!(error, IndexSettingsParseError::FilterParseError("bad_filter".to_string(), FilterParseError::UnrecognisedType("foo".to_string())));
    }
}
