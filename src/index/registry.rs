use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use index::Index;


#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct IndexRef(u32);


#[derive(Debug)]
pub struct IndexRegistry {
    ref_counter: u32,
    indices: HashMap<IndexRef, Index>,
    pub aliases: HashMap<String, IndexRef>,
}


impl IndexRegistry {
    pub fn new() -> IndexRegistry {
        IndexRegistry {
            ref_counter: 1,
            indices: HashMap::new(),
            aliases: HashMap::new(),
        }
    }

    pub fn insert(&mut self, index: Index) -> IndexRef {
        let index_ref = IndexRef(self.ref_counter);
        self.ref_counter += 1;

        self.indices.insert(index_ref, index);

        index_ref
    }
}


impl Deref for IndexRegistry {
    type Target = HashMap<IndexRef, Index>;

    fn deref(&self) -> &HashMap<IndexRef, Index> {
        &self.indices
    }
}


impl DerefMut for IndexRegistry {
    fn deref_mut(&mut self) -> &mut HashMap<IndexRef, Index> {
        &mut self.indices
    }
}
