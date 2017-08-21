pub mod analysis_tokenizer;
pub mod analysis_filter;
pub mod analysis_analyzer;

use serde_json;

use index::metadata::IndexMetadata;
use mapping::parse::{MappingParseError, parse as parse_mapping};

use self::analysis_tokenizer::{TokenizerParseError, parse as parse_tokenizer};
use self::analysis_filter::{FilterParseError, parse as parse_filter};
use self::analysis_analyzer::{AnalyzerParseError, parse as parse_analyzer};


#[derive(Debug, PartialEq)]
pub enum IndexMetadataParseError {
    ExpectedObject,
    TokenizerParseError(String, TokenizerParseError),
    FilterParseError(String, FilterParseError),
    AnalyzerParseError(String, AnalyzerParseError),
    MappingParseError(String, MappingParseError),
}


pub fn parse(metadata: &mut IndexMetadata, data: serde_json::Value) -> Result<(), IndexMetadataParseError> {
    let data = match data.as_object() {
        Some(object) => object,
        None => {
            return Err(IndexMetadataParseError::ExpectedObject);
        }
    };

    if let Some(settings) = data.get("settings") {
        let settings = match settings.as_object() {
            Some(object) => object,
            None => return Err(IndexMetadataParseError::ExpectedObject),
        };

        if let Some(analysis) = settings.get("analysis") {
            let analysis = match analysis.as_object() {
                Some(object) => object,
                None => return Err(IndexMetadataParseError::ExpectedObject),
            };

            // Tokenisers
            if let Some(tokenizer_data) = analysis.get("tokenizer") {
                let tokenizer_data = match tokenizer_data.as_object() {
                    Some(object) => object,
                    None => return Err(IndexMetadataParseError::ExpectedObject),
                };

                for (name, data) in tokenizer_data {
                    let tokenizer = match parse_tokenizer(data) {
                        Ok(tokenizer) => tokenizer,
                        Err(e) => return Err(IndexMetadataParseError::TokenizerParseError(name.to_string(), e)),
                    };

                    metadata.insert_tokenizer(name.clone(), tokenizer);
                }
            }

            // Token filters
            if let Some(filter_data) = analysis.get("filter") {
                let filter_data = match filter_data.as_object() {
                    Some(object) => object,
                    None => return Err(IndexMetadataParseError::ExpectedObject),
                };

                for (name, data) in filter_data {
                    let filter = match parse_filter(data) {
                        Ok(filter) => filter,
                        Err(e) => return Err(IndexMetadataParseError::FilterParseError(name.to_string(), e)),
                    };

                    metadata.insert_filter(name.clone(), filter);
                }
            }

            // Analyzers
            if let Some(analyzer_data) = analysis.get("analyzer") {
                let analyzer_data = match analyzer_data.as_object() {
                    Some(object) => object,
                    None => return Err(IndexMetadataParseError::ExpectedObject),
                };

                for (name, data) in analyzer_data {
                    let analyzer = match parse_analyzer(data, &metadata) {
                        Ok(analyzer) => analyzer,
                        Err(e) => return Err(IndexMetadataParseError::AnalyzerParseError(name.to_string(), e)),
                    };

                    metadata.insert_analyzer(name.clone(), analyzer);
                }
            }
        }
    }

    if let Some(mappings) = data.get("mappings") {
        let mappings = match mappings.as_object() {
            Some(object) => object,
            None => return Err(IndexMetadataParseError::ExpectedObject),
        };

        for (name, data) in mappings {
            let mapping_builder = match parse_mapping(data) {
                Ok(mapping) => mapping,
                Err(e) => return Err(IndexMetadataParseError::MappingParseError(name.to_string(), e)),
            };
            let mapping = mapping_builder.build(&metadata);
            metadata.mappings.insert(name.clone(), mapping);
        }
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use serde_json;

    use analysis::ngram_generator::Edge;
    use analysis::tokenizers::TokenizerSpec;
    use analysis::filters::FilterSpec;
    use analysis::AnalyzerSpec;
    use mapping::parse::MappingParseError;
    use index::metadata::IndexMetadata;

    use super::{parse, IndexMetadataParseError};
    use super::analysis_tokenizer::TokenizerParseError;
    use super::analysis_filter::FilterParseError;

    #[test]
    fn test_default() {
        let mut metadata = IndexMetadata::default();
        parse(&mut metadata, serde_json::from_str("
        {}
        ").unwrap()).expect("parse() returned an error");

        assert_eq!(metadata.tokenizers().len(), 2);
        assert_eq!(metadata.filters().len(), 2);
        assert_eq!(metadata.analyzers().len(), 1);

        // Check builtin tokenizers
        let standard_tokenizer = metadata.tokenizers().get("standard").expect("'standard' tokenizer wasn't created");
        assert_eq!(*standard_tokenizer, TokenizerSpec::Standard);

        let lowercase_tokenizer = metadata.tokenizers().get("lowercase").expect("'lowercase' tokenizer wasn't created");
        assert_eq!(*lowercase_tokenizer, TokenizerSpec::Lowercase);

        // Check builtin filters
        let lowercase_filter = metadata.filters().get("lowercase").expect("'lowercase' filter wasn't created");
        assert_eq!(*lowercase_filter, FilterSpec::Lowercase);

        let asciifolding_filter = metadata.filters().get("asciifolding").expect("'asciifolding' filter wasn't created");
        assert_eq!(*asciifolding_filter, FilterSpec::ASCIIFolding);

        // Check builtin analyzers
        let standard_analyzer = metadata.analyzers().get("standard").expect("'standard' analyzer wasn't created");
        assert_eq!(*standard_analyzer, AnalyzerSpec {
            tokenizer: TokenizerSpec::Standard,
            filters: vec![
                FilterSpec::Lowercase,
                FilterSpec::ASCIIFolding,
            ]
        });
    }

    #[test]
    fn test_custom_analyser() {
        let mut metadata = IndexMetadata::default();
        parse(&mut metadata, serde_json::from_str("
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
        ").unwrap()).expect("parse() returned an error");

        assert_eq!(metadata.tokenizers().len(), 6);
        assert_eq!(metadata.filters().len(), 6);
        assert_eq!(metadata.analyzers().len(), 1);

        // Check tokenizers
        let ngram_tokenizer = metadata.tokenizers().get("ngram_tokenizer").expect("'ngram_tokenizer' wasn't created");
        assert_eq!(*ngram_tokenizer, TokenizerSpec::NGram {
            min_size: 3,
            max_size: 15,
            edge: Edge::Neither,
        });

        let edgengram_tokenizer = metadata.tokenizers().get("edgengram_tokenizer").expect("'edgengram_tokenizer' wasn't created");
        assert_eq!(*edgengram_tokenizer, TokenizerSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Left,
        });

        let edgengram_tokenizer_side_front = metadata.tokenizers().get("edgengram_tokenizer_side_front").expect("'edgengram_tokenizer_side_front' wasn't created");
        assert_eq!(*edgengram_tokenizer_side_front, TokenizerSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Left,
        });

        let edgengram_tokenizer_side_back = metadata.tokenizers().get("edgengram_tokenizer_side_back").expect("'edgengram_tokenizer_side_back' wasn't created");
        assert_eq!(*edgengram_tokenizer_side_back, TokenizerSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Right,
        });

        // Check filters
        let ngram_filter = metadata.filters().get("ngram_filter").expect("'ngram_filter' wasn't created");
        assert_eq!(*ngram_filter, FilterSpec::NGram {
            min_size: 3,
            max_size: 15,
            edge: Edge::Neither,
        });

        let edgengram_filter = metadata.filters().get("edgengram_filter").expect("'edgengram_filter' wasn't created");
        assert_eq!(*edgengram_filter, FilterSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Left,
        });

        let edgengram_filter_side_front = metadata.filters().get("edgengram_filter_side_front").expect("'edgengram_filter_side_front' wasn't created");
        assert_eq!(*edgengram_filter_side_front, FilterSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Left,
        });

        let edgengram_filter_side_back = metadata.filters().get("edgengram_filter_side_back").expect("'edgengram_filter_side_back' wasn't created");
        assert_eq!(*edgengram_filter_side_back, FilterSpec::NGram {
            min_size: 2,
            max_size: 15,
            edge: Edge::Right,
        });
    }

    #[test]
    fn test_custom_analyser_bad_tokenizer_type() {
        let mut metadata = IndexMetadata::default();
        let error = parse(&mut metadata, serde_json::from_str("
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
        ").unwrap()).err().expect("parse() was supposed to return an error, but didn't");

        assert_eq!(error, IndexMetadataParseError::TokenizerParseError("bad_tokenizer".to_string(), TokenizerParseError::UnrecognisedType("foo".to_string())));
    }

    #[test]
    fn test_custom_analyser_bad_filter_type() {
        let mut metadata = IndexMetadata::default();
        let error = parse(&mut metadata, serde_json::from_str("
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
        ").unwrap()).err().expect("parse() was supposed to return an error, but didn't");

        assert_eq!(error, IndexMetadataParseError::FilterParseError("bad_filter".to_string(), FilterParseError::UnrecognisedType("foo".to_string())));
    }

    #[test]
    fn test_mapping() {
        let mut metadata = IndexMetadata::default();
        parse(&mut metadata, json!({
            "mappings": {
                "test_mapping": {
                    "properties": {
                        "test_field": {
                            "type": "string",
                        }
                    }
                }
            }
        })).expect("parse() returned an error");

        assert_eq!(metadata.mappings.len(), 1);
    }

    #[test]
    fn test_mapping_error() {
        let mut metadata = IndexMetadata::default();
        let error = parse(&mut metadata, json!({
            "mappings": {
                "test_mapping": {
                    "foo": []
                }
            }
        })).err().expect("parse() was supposed to return an error, but didn't");

        assert_eq!(error, IndexMetadataParseError::MappingParseError("test_mapping".to_string(), MappingParseError::UnrecognisedKeys(vec!["foo".to_string()])));
    }
}
