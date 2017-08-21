use std::collections::HashMap;

use mapping::{Mapping, MappingProperty, FieldMapping, NestedMapping, FieldType, get_standard_analyzer};
use index::metadata::IndexMetadata;


#[derive(Debug, PartialEq)]
pub struct FieldMappingBuilder {
    pub field_type: FieldType,
    pub is_indexed: bool,
    pub is_analyzed: bool,
    pub is_stored: bool,
    pub is_in_all: bool,
    pub boost: f64,
    pub base_analyzer: Option<String>,
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
            base_analyzer: None,
            index_analyzer: None,
            search_analyzer: None,
        }
    }
}


impl FieldMappingBuilder {
    pub fn build(&self, index_metadata: &IndexMetadata) -> FieldMapping {
        let base_analyzer = match self.base_analyzer {
            Some(ref base_analyzer) => {
                match index_metadata.analyzers().get(base_analyzer) {
                    Some(analyzer) => Some(analyzer),
                    None => None,
                }
            }
            None => None,
        };

        let index_analyzer = if self.is_analyzed {
            match self.index_analyzer {
                Some(ref index_analyzer) => {
                    match index_metadata.analyzers().get(index_analyzer) {
                        Some(analyzer) => Some(analyzer.clone()),
                        None => {
                            // TODO: error
                            Some(base_analyzer.cloned().unwrap_or_else(|| index_metadata.get_default_index_analyzer()))
                        },
                    }
                }
                None => Some(base_analyzer.cloned().unwrap_or_else(|| index_metadata.get_default_index_analyzer())),
            }
        } else {
            None
        };

        let search_analyzer = if self.is_analyzed {
            match self.search_analyzer {
                Some(ref search_analyzer) => {
                    match index_metadata.analyzers().get(search_analyzer) {
                        Some(analyzer) => Some(analyzer.clone()),
                        None => {
                            // TODO: error
                            Some(base_analyzer.cloned().unwrap_or_else(|| index_metadata.get_default_search_analyzer()))
                        },
                    }
                }
                None => Some(base_analyzer.cloned().unwrap_or_else(|| index_metadata.get_default_search_analyzer())),
            }
        } else {
            None
        };

        FieldMapping {
            data_type: self.field_type,
            index_ref: None,
            is_indexed: self.is_indexed,
            is_stored: self.is_stored,
            is_in_all: self.is_in_all,
            boost: self.boost,
            index_analyzer: index_analyzer,
            search_analyzer: search_analyzer,
        }
    }
}


#[derive(Debug, PartialEq)]
pub struct NestedMappingBuilder {
    pub properties: HashMap<String, MappingPropertyBuilder>,
}


impl Default for NestedMappingBuilder {
    fn default() -> NestedMappingBuilder {
        NestedMappingBuilder {
            properties: HashMap::new(),
        }
    }
}


impl NestedMappingBuilder {
    pub fn build(&self, index_metadata: &IndexMetadata) -> NestedMapping {
        // Insert fields
        let mut properties = HashMap::new();
        for (field_name, builder) in self.properties.iter() {
            match *builder {
                MappingPropertyBuilder::Field(ref field_builder) => {
                     properties.insert(field_name.to_string(), MappingProperty::Field(field_builder.build(index_metadata)));
                }
                MappingPropertyBuilder::NestedMapping(ref nested_mapping_builder) => {
                    properties.insert(field_name.to_string(), MappingProperty::NestedMapping(Box::new(nested_mapping_builder.build(index_metadata))));
                }
            }
        }

        NestedMapping {
            properties: properties,
        }
    }
}


#[derive(Debug, PartialEq)]
pub enum MappingPropertyBuilder {
    Field(FieldMappingBuilder),
    NestedMapping(Box<NestedMappingBuilder>),
}


#[derive(Debug, PartialEq)]
pub struct MappingBuilder {
    pub properties: HashMap<String, MappingPropertyBuilder>,
}


impl MappingBuilder {
    pub fn build(&self, index_metadata: &IndexMetadata) -> Mapping {
        // Insert fields
        let mut properties = HashMap::new();
        for (field_name, builder) in self.properties.iter() {
            match *builder {
                MappingPropertyBuilder::Field(ref field_builder) => {
                     properties.insert(field_name.to_string(), MappingProperty::Field(field_builder.build(index_metadata)));
                }
                MappingPropertyBuilder::NestedMapping(ref nested_mapping_builder) => {
                    properties.insert(field_name.to_string(), MappingProperty::NestedMapping(Box::new(nested_mapping_builder.build(index_metadata))));
                }
            }
        }

        // Insert _all field
        if !properties.contains_key("_all") {
            // TODO: Support disabling the _all field
            properties.insert("_all".to_string(), MappingProperty::Field(
                FieldMapping {
                    data_type: FieldType::String,
                    is_stored: false,
                    is_in_all: false,
                    index_analyzer: Some(get_standard_analyzer()),
                    search_analyzer: Some(get_standard_analyzer()),
                    .. FieldMapping::default()
                }
            ));
        }

        Mapping {
            properties: properties,
        }
    }
}


#[cfg(test)]
mod tests {
    use analysis::AnalyzerSpec;
    use analysis::tokenizers::TokenizerSpec;
    use analysis::filters::FilterSpec;
    use mapping::{Mapping, MappingProperty, FieldMapping, FieldType, get_standard_analyzer};
    use index::metadata::IndexMetadata;

    use super::{MappingBuilder, MappingPropertyBuilder, FieldMappingBuilder};

    #[test]
    fn test_build() {
        let index_metadata = IndexMetadata::default();
        let builder = MappingBuilder {
            properties: hashmap! {
                "title".to_string() => MappingPropertyBuilder::Field(
                    FieldMappingBuilder {
                        field_type: FieldType::String,
                        is_in_all: true,
                        boost: 2.0f64,
                        ..FieldMappingBuilder::default()
                    }
                )
            },
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, Mapping {
            properties: hashmap! {
                "title".to_string() => MappingProperty::Field(FieldMapping {
                    data_type: FieldType::String,
                    is_in_all: true,
                    boost: 2.0f64,
                    index_analyzer: Some(get_standard_analyzer()),
                    search_analyzer: Some(get_standard_analyzer()),
                    ..FieldMapping::default()
                }),
                "_all".to_string() => MappingProperty::Field(FieldMapping {
                    data_type: FieldType::String,
                    is_in_all: false,
                    index_analyzer: Some(get_standard_analyzer()),
                    search_analyzer: Some(get_standard_analyzer()),
                    ..FieldMapping::default()
                })
            }
        });
    }

    #[test]
    fn test_build_no_fields() {
        let index_metadata = IndexMetadata::default();
        let builder = MappingBuilder {
            properties: hashmap! {},
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, Mapping {
            properties: hashmap! {
                "_all".to_string() => MappingProperty::Field(FieldMapping {
                    data_type: FieldType::String,
                    is_in_all: false,
                    index_analyzer: Some(get_standard_analyzer()),
                    search_analyzer: Some(get_standard_analyzer()),
                    ..FieldMapping::default()
                })
            }
        });
    }

    #[test]
    fn test_build_override_all_field() {
        let index_metadata = IndexMetadata::default();
        let builder = MappingBuilder {
            properties: hashmap! {
                "_all".to_string() => MappingPropertyBuilder::Field(
                    FieldMappingBuilder {
                        field_type: FieldType::String,
                        boost: 2.0f64,
                        ..FieldMappingBuilder::default()
                    }
                )
            },
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, Mapping {
            properties: hashmap! {
                "_all".to_string() => MappingProperty::Field(FieldMapping {
                    data_type: FieldType::String,
                    boost: 2.0f64,
                    index_analyzer: Some(get_standard_analyzer()),
                    search_analyzer: Some(get_standard_analyzer()),
                    ..FieldMapping::default()
                })
            }
        });
    }

    #[test]
    fn test_build_field() {
        let index_metadata = IndexMetadata::default();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            index_analyzer: Some(get_standard_analyzer()),
            search_analyzer: Some(get_standard_analyzer()),
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_types() {
        let index_metadata = IndexMetadata::default();
        let builder = FieldMappingBuilder {
            field_type: FieldType::Integer,
            is_analyzed: false,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::Integer,
            index_analyzer: None,
            search_analyzer: None,
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_stored() {
        let index_metadata = IndexMetadata::default();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            is_stored: true,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            is_stored: true,
            index_analyzer: Some(get_standard_analyzer()),
            search_analyzer: Some(get_standard_analyzer()),
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_is_in_all() {
        let index_metadata = IndexMetadata::default();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            is_in_all: false,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            is_in_all: false,
            index_analyzer: Some(get_standard_analyzer()),
            search_analyzer: Some(get_standard_analyzer()),
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_boost() {
        let index_metadata = IndexMetadata::default();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            boost: 2.0f64,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            boost: 2.0f64,
            index_analyzer: Some(get_standard_analyzer()),
            search_analyzer: Some(get_standard_analyzer()),
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_analyzer_default() {
        let index_metadata = IndexMetadata::default();
        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            index_analyzer: Some(get_standard_analyzer()),
            search_analyzer: Some(get_standard_analyzer()),
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
        let mut index_metadata = IndexMetadata::default();
        index_metadata.insert_analyzer("my-analyzer".to_string(), build_test_analyzer());

        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            base_analyzer: Some("my-analyzer".to_string()),
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            index_analyzer: Some(build_test_analyzer()),
            search_analyzer: Some(build_test_analyzer()),
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_custom_index_analyzer() {
        let mut index_metadata = IndexMetadata::default();
        index_metadata.insert_analyzer("my-analyzer".to_string(), build_test_analyzer());

        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            index_analyzer: Some("my-analyzer".to_string()),
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            index_analyzer: Some(build_test_analyzer()),
            search_analyzer: Some(get_standard_analyzer()),
            ..FieldMapping::default()
        });
    }

    #[test]
    fn test_build_field_custom_search_analyzer() {
        let mut index_metadata = IndexMetadata::default();
        index_metadata.insert_analyzer("my-analyzer".to_string(), build_test_analyzer());

        let builder = FieldMappingBuilder {
            field_type: FieldType::String,
            search_analyzer: Some("my-analyzer".to_string()),
            ..FieldMappingBuilder::default()
        };

        let mapping = builder.build(&index_metadata);

        assert_eq!(mapping, FieldMapping {
            data_type: FieldType::String,
            index_analyzer: Some(get_standard_analyzer()),
            search_analyzer: Some(build_test_analyzer()),
            ..FieldMapping::default()
        });
    }
}
