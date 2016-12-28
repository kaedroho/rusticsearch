use rustc_serialize::json::Json;

use analysis::AnalyzerSpec;
use analysis::registry::AnalyzerRegistry;


#[derive(Debug, PartialEq)]
pub enum AnalyzerParseError {
    ExpectedObject,
    ExpectedString,
    ExpectedArray,
    ExpectedKey(String),
    UnrecognisedAnalyzerType(String),
    UnrecognisedTokenizer(String),
    UnrecognisedFilter(String),
}


pub fn parse(json: &Json, analyzers: &AnalyzerRegistry) -> Result<AnalyzerSpec, AnalyzerParseError> {
    let data = try!(json.as_object().ok_or(AnalyzerParseError::ExpectedObject));

    // Get type
    let analyzer_type_json = try!(data.get("type").ok_or(AnalyzerParseError::ExpectedKey("type".to_string())));
    let analyzer_type = try!(analyzer_type_json.as_string().ok_or(AnalyzerParseError::ExpectedString));

    match analyzer_type {
        "custom" => {
            // Get tokenizer
            let tokenizer_name = match data.get("tokenizer") {
                Some(tokenizer_json) => {
                    match tokenizer_json.as_string() {
                        Some(tokenizer) => tokenizer,
                        None => return Err(AnalyzerParseError::ExpectedString),
                    }
                }
                None => return Err(AnalyzerParseError::ExpectedKey("tokenizer".to_string())),
            };

            let tokenizer_spec = match analyzers.get_tokenizer(tokenizer_name) {
                Some(tokenizer_spec) => tokenizer_spec,
                None => return Err(AnalyzerParseError::UnrecognisedTokenizer(tokenizer_name.to_string())),
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
                                    let filter_spec = match analyzers.get_filter(filter_name) {
                                        Some(filter_spec) => filter_spec,
                                        None => return Err(AnalyzerParseError::UnrecognisedFilter(filter_name.to_string())),
                                    };

                                    analyzer_spec.filters.push(filter_spec.clone());
                                }
                                None => return Err(AnalyzerParseError::ExpectedString),
                            }
                        }
                    },
                    None => return Err(AnalyzerParseError::ExpectedArray),
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
        _ => Err(AnalyzerParseError::UnrecognisedAnalyzerType(analyzer_type.to_string())),
    }
}
