use std::collections::HashMap;

use abra::analysis::registry::AnalyzerRegistry;

use mapping::{Mapping, FieldMapping, FieldType, get_standard_analyzer};


#[derive(Debug, PartialEq)]
pub struct FieldMappingBuilder {
    pub field_type: FieldType,
    pub is_indexed: bool,
    pub is_analyzed: bool,
    pub is_stored: bool,
    pub is_in_all: bool,
    pub boost: f64,
    pub base_analyzer: String,
    pub index_analyzer: Option<String>,
    pub search_analyzer: Option<String>
}


impl Default for FieldMappingBuilder {
    fn default() -> FieldMappingBuilder {
        FieldMappingBuilder {
            field_type: FieldType::String,
            is_indexed: true,
            is_analyzed: true,
            is_stored: false,
            is_in_all: true,
            boost: 1.0f64,
            base_analyzer: "default".to_string(),
            index_analyzer: None,
            search_analyzer: None,
        }
    }
}


impl FieldMappingBuilder {
    pub fn build(&self, analyzers: &AnalyzerRegistry) -> FieldMapping {
        let base_analyzer = match analyzers.get(&self.base_analyzer) {
            Some(analyzer) => analyzer.clone(),
            None => get_standard_analyzer(),
        };

        let index_analyzer = match self.index_analyzer {
            Some(ref index_analyzer) => {
                match analyzers.get(index_analyzer) {
                    Some(analyzer) => Some(analyzer.clone()),
                    None => None,
                }
            }
            None => None,
        };

        let search_analyzer = match self.search_analyzer {
            Some(ref search_analyzer) => {
                match analyzers.get(search_analyzer) {
                    Some(analyzer) => Some(analyzer.clone()),
                    None => None,
                }
            }
            None => None,
        };

        FieldMapping {
            data_type: self.field_type,
            index_ref: None,
            is_stored: self.is_stored,
            is_in_all: self.is_in_all,
            boost: self.boost,
            base_analyzer: base_analyzer,
            index_analyzer: index_analyzer,
            search_analyzer: search_analyzer,
        }
    }
}


#[derive(Debug, PartialEq)]
pub struct MappingBuilder {
    pub properties: HashMap<String, FieldMappingBuilder>,
}


impl MappingBuilder {
    pub fn build(&self, analyzers: &AnalyzerRegistry) -> Mapping {
        // Insert fields
        let mut fields = HashMap::new();
        for (field_name, field_builder) in self.properties.iter() {
            fields.insert(field_name.to_string(), field_builder.build(analyzers));
        }

        // Insert _all field
        if !fields.contains_key("_all") {
            // TODO: Support disabling the _all field
            fields.insert("_all".to_string(), FieldMapping {
                data_type: FieldType::String,
                is_stored: false,
                is_in_all: false,
                .. FieldMapping::default()
            });
        }

        Mapping {
            fields: fields,
        }
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use abra::analysis::registry::AnalyzerRegistry;
    use abra::analysis::AnalyzerSpec;
    use abra::analysis::tokenizers::TokenizerSpec;
    use abra::analysis::filters::FilterSpec;

    use mapping::{Mapping, FieldMapping, FieldType, get_standard_analyzer};

    use super::{MappingBuilder, FieldMappingBuilder};

    #[test]
    fn test_build() {
        let analyzers = AnalyzerRegistry::new();
        let builder = MappingBuilder {
            properties: hashmap! {
                "title".to_string() => FieldMappingBuilder {
                    field_type: FieldType::String,
                    is_in_all: true,
                    boost: 2.0f64,
                    ..FieldMappingBuilder::default()
                }
            },
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, Mapping {
            fields: hashmap! {
                "title".to_string() => FieldMapping {
                    data_type: FieldType::String,
                    is_in_all: true,
                    boost: 2.0f64,
                    ..FieldMapping::default()
                },
                "_all".to_string() => FieldMapping {
                    data_type: FieldType::String,
                    is_in_all: false,
                    ..FieldMapping::default()
                }
            }
        });
    }

    #[test]
    fn test_build_no_fields() {
        let analyzers = AnalyzerRegistry::new();
        let builder = MappingBuilder {
            properties: hashmap! {},
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, Mapping {
            fields: hashmap! {
                "_all".to_string() => FieldMapping {
                    data_type: FieldType::String,
                    is_in_all: false,
                    ..FieldMapping::default()
                }
            }
        });
    }

    #[test]
    fn test_build_override_all_field() {
        let analyzers = AnalyzerRegistry::new();
        let builder = MappingBuilder {
            properties: hashmap! {
                "_all".to_string() => FieldMappingBuilder {
                    field_type: FieldType::String,
                    boost: 2.0f64,
                    ..FieldMappingBuilder::default()
                }
            },
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, Mapping {
            fields: hashmap! {
                "_all".to_string() => FieldMapping {
                    data_type: FieldType::String,
                    boost: 2.0f64,
                    ..FieldMapping::default()
                }
            }
        });
    }

    #[test]
    fn test_build_field() {
        let analyzers = AnalyzerRegistry::new();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_types() {
        let analyzers = AnalyzerRegistry::new();
        let builder = FieldMappingBuilder {
            field_type: FieldType::Integer,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::Integer,
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_stored() {
        let analyzers = AnalyzerRegistry::new();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            is_stored: true,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            is_stored: true,
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_is_in_all() {
        let analyzers = AnalyzerRegistry::new();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            is_in_all: false,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            is_in_all: false,
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_boost() {
        let analyzers = AnalyzerRegistry::new();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            boost: 2.0f64,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            boost: 2.0f64,
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_analyzer_default() {
        let analyzers = AnalyzerRegistry::new();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            base_analyzer: get_standard_analyzer(),
            index_analyzer: None,
            search_analyzer: None,
            ..FieldMapping::default()
        });
    }

    fn build_test_analyzer() -> AnalyzerSpec {
        AnalyzerSpec {
            tokenizer: TokenizerSpec::Standard,
            filters: vec![
                FilterSpec::Lowercase,
            ]
        }
    }

    #[test]
    fn test_build_field_custom_base_analyzer() {
        let mut analyzers = AnalyzerRegistry::new();
        analyzers.insert("my-analyzer".to_string(), build_test_analyzer());

        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: "my-analyzer".to_string(),
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            base_analyzer: build_test_analyzer(),
            index_analyzer: None,
            search_analyzer: None,
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_custom_index_analyzer() {
        let mut analyzers = AnalyzerRegistry::new();
        analyzers.insert("my-analyzer".to_string(), build_test_analyzer());

        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            index_analyzer: Some("my-analyzer".to_string()),
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            base_analyzer: get_standard_analyzer(),
            index_analyzer: Some(build_test_analyzer()),
            search_analyzer: None,
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_custom_search_analyzer() {
        let mut analyzers = AnalyzerRegistry::new();
        analyzers.insert("my-analyzer".to_string(), build_test_analyzer());

        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            search_analyzer: Some("my-analyzer".to_string()),
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&analyzers);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            base_analyzer: get_standard_analyzer(),
            index_analyzer: None,
            search_analyzer: Some(build_test_analyzer()),
            ..FieldMapping::default()
        });
    }
}
