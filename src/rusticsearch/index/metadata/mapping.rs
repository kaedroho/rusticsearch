use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use mapping::{Mapping, FieldMapping};


#[derive(Debug)]
pub struct MappingRegistry {
    mappings: HashMap<String, Mapping>,
}


impl MappingRegistry {
    pub fn new() -> MappingRegistry {
        MappingRegistry {
            mappings: HashMap::new(),
        }
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldMapping> {
        for mapping in self.mappings.values() {
            if let Some(ref field_mapping) = mapping.fields.get(name) {
                return Some(field_mapping);
            }
        }

        None
    }
}


impl Deref for MappingRegistry {
    type Target = HashMap<String, Mapping>;

    fn deref(&self) -> &HashMap<String, Mapping> {
        &self.mappings
    }
}


impl DerefMut for MappingRegistry {
    fn deref_mut(&mut self) -> &mut HashMap<String, Mapping> {
        &mut self.mappings
    }
}
