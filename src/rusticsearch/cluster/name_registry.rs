use std::collections::HashMap;
use std::collections::hash_map::Iter as HashMapIter;

use super::IndexRef;


#[derive(Debug)]
enum Name {
    /// This is the canonical name of an index
    Canonical(IndexRef),

    /// This is an alias
    Alias(Vec<IndexRef>),
}


#[derive(Debug)]
pub struct NameRegistry {
    names: HashMap<String, Name>,
}


impl NameRegistry {
    pub fn new() -> NameRegistry {
        NameRegistry {
            names: HashMap::new(),
        }
    }

    pub fn insert_canonical(&mut self, name: String, index_ref: IndexRef) -> Result<(), ()> {
        if let Some(_) = self.names.get(&name) {
            return Err(());
        }

        self.names.insert(name, Name::Canonical(index_ref));
        Ok(())
    }

    pub fn delete_canonical(&mut self, name: &str, index_ref: IndexRef) -> Result<(), ()> {
        if let Some(&Name::Canonical(actual_index_ref)) = self.names.get(name) {
            if actual_index_ref != index_ref {
                return Err(());
            }
        } else {
            return Err(());
        }

        self.names.remove(name);
        Ok(())
    }

    pub fn insert_alias(&mut self, name: String, indices: Vec<IndexRef>) -> Result<(), ()> {
        if let Some(_) = self.names.get(&name) {
            return Err(());
        }

        self.names.insert(name, Name::Alias(indices));
        Ok(())
    }

    pub fn insert_or_replace_alias(&mut self, name: String, indices: Vec<IndexRef>) -> Result<bool, ()> {
        if let Some(&Name::Canonical(_)) = self.names.get(&name) {
            // Cannot replace if it is a canonical name
            return Err(());
        }

        let old_indices = self.names.insert(name, Name::Alias(indices));
        match old_indices {
            Some(Name::Alias(_)) => {
                 Ok(false)
            }
            Some(Name::Canonical(_)) => {
                unreachable!();
            }
            None => {
                Ok(true)
            }
        }
    }

    pub fn delete_alias(&mut self, name: &str, index_ref: IndexRef) -> Result<bool, ()> {
        let mut remove_alias = false;

        match self.names.get_mut(name) {
            Some(&mut Name::Alias(ref mut indices)) => {
                // Remove index from alias
                let index = match indices.iter().position(|ir| *ir == index_ref) {
                    Some(index) => index,
                    None => return Ok(false),
                };

                indices.remove(index);

                if indices.is_empty() {
                    remove_alias = true;
                }
            }
            Some(&mut Name::Canonical(_)) => {
                return Err(());
            }
            None => {}
        }

        if remove_alias {
            self.names.remove(name);
        }

        Ok(remove_alias)
    }

    pub fn delete_alias_whole(&mut self, name: &str) -> Result<bool, ()> {
        if let Some(&Name::Canonical(_)) = self.names.get(name) {
            return Err(());
        }

        let alias = self.names.remove(name);
        Ok(alias.is_some())
    }

    pub fn find(&self, selector: &str) -> Vec<IndexRef> {
        let mut indices = Vec::new();

        // Find name
        let name = self.names.get(selector);

        // Resolve the name if we have one
        if let Some(name) = name {
            match *name {
                Name::Canonical(ref index_ref) => indices.push(*index_ref),
                Name::Alias(ref alias_indices) => indices.append(&mut alias_indices.clone()),
            }
        }

        indices
    }

    pub fn find_canonical(&self, name: &str) -> Option<IndexRef> {
        let name = self.names.get(name);

        match name {
            Some(&Name::Canonical(index_ref)) => Some(index_ref),
            Some(&Name::Alias(_)) | None => None,
        }
    }

    pub fn iter_index_aliases<'a>(&'a self, index_ref: IndexRef) -> IndexAliasesIterator<'a> {
        IndexAliasesIterator {
            index_ref: index_ref,
            names_iterator: self.names.iter(),
        }
    }
}


pub struct IndexAliasesIterator<'a> {
    index_ref: IndexRef,
    names_iterator: HashMapIter<'a, String, Name>,
}


impl<'a> Iterator for IndexAliasesIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        loop {
            match self.names_iterator.next() {
                Some((name, &Name::Alias(ref indices))) => {
                    if indices.iter().any(|ir| *ir == self.index_ref) {
                        return Some(name);
                    }
                }
                Some((_, &Name::Canonical(_))) => {}
                None => return None
            }
        }
    }
}
