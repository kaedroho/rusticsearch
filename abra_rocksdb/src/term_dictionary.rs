use std::str;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::BTreeMap;

use rocksdb::{DB, Writable, IteratorMode, Direction};
use abra::Term;

use key_builder::KeyBuilder;


#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct TermRef(u32);


impl TermRef {
    pub fn ord(&self) -> u32 {
        self.0
    }
}


pub struct TermDictionary {
    next_term_ref: AtomicU32,
    terms: RwLock<BTreeMap<Vec<u8>, TermRef>>,
}


impl TermDictionary {
    pub fn new(db: &DB) -> TermDictionary {
        // Next term ref
        db.put(b".next_term_ref", b"1");

        TermDictionary {
            next_term_ref: AtomicU32::new(1),
            terms: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn open(db: &DB) -> TermDictionary {
        let next_term_ref = match db.get(b".next_term_ref") {
            Ok(Some(next_term_ref)) => {
                next_term_ref.to_utf8().unwrap().parse::<u32>().unwrap()
            }
            Ok(None) => 1,  // TODO: error
            Err(_) => 1,  // TODO: error
        };

        // Read dictionary
        let mut terms = BTreeMap::new();
        for (k, v) in db.iterator(IteratorMode::From(b"t", Direction::Forward)) {
            if k[0] != b't' {
                break;
            }

            let term_ref = TermRef(str::from_utf8(&v).unwrap().parse::<u32>().unwrap());
            terms.insert(k[1..].to_vec(), term_ref);
        }

        TermDictionary {
            next_term_ref: AtomicU32::new(next_term_ref),
            terms: RwLock::new(terms),
        }
    }

    pub fn get(&self, term_bytes: &Vec<u8>) -> Option<TermRef> {
        self.terms.read().unwrap().get(term_bytes).cloned()
    }

    pub fn get_or_create(&mut self, db: &DB, term: &Term) -> TermRef {
        let term_bytes = term.to_bytes();

        if let Some(term_ref) = self.get(&term_bytes) {
            return term_ref;
        }

        // Term doesn't exist in the term dictionary

        // Increment next_term_ref
        let next_term_ref = self.next_term_ref.fetch_add(1, Ordering::SeqCst);
        db.put(b".next_term_ref", (next_term_ref + 1).to_string().as_bytes());

        // Create term ref
        let term_ref = TermRef(next_term_ref);

        // Get exclusive lock to term dictionary
        let mut terms = self.terms.write().unwrap();

        // It's possible that another thread has written the term to the dictionary
        // since we checked earlier. If this is the case, We should forget about
        // writing our TermRef and use the one that has been inserted already.
        if let Some(term_ref) = terms.get(&term_bytes) {
            return *term_ref;
        }

        // Write it to the on-disk term dictionary
        let mut kb = KeyBuilder::term_dict_mapping(&term_bytes);
        db.put(kb.key(), next_term_ref.to_string().as_bytes());

        // Write it to the term dictionary
        terms.insert(term_bytes, term_ref);

        term_ref
    }
}
