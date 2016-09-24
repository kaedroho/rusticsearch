use std::collections::HashMap;

use rustc_serialize::json::Json;

use analysis::AnalyzerSpec;
use analysis::tokenizers::TokenizerSpec;
use analysis::filters::FilterSpec;

use super::IndexSettingsParseError;


pub fn parse(data: &Json, tokenizers: &HashMap<String, TokenizerSpec>, filters: &HashMap<String, FilterSpec>) -> Result<AnalyzerSpec, IndexSettingsParseError> {
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
