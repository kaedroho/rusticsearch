use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use system::index::Index;


#[derive(Debug)]
pub struct IndexRegistry {
    indices: HashMap<String, Index>,
}


impl IndexRegistry {
    pub fn new() -> IndexRegistry {
        IndexRegistry {
            indices: HashMap::new(),
        }
    }
}


impl Deref for IndexRegistry {
    type Target = HashMap<String, Index>;

    fn deref(&self) -> &HashMap<String, Index> {
        &self.indices
    }
}


impl DerefMut for IndexRegistry {
    fn deref_mut(&mut self) -> &mut HashMap<String, Index> {
        &mut self.indices
    }
}
