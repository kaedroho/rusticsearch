use std::ops::{Deref, DerefMut};
use std::collections::HashMap;

use roaring::RoaringBitmap;

use search::term::Term;
use search::token::Token;

#[derive(Debug, Clone, PartialEq)]
pub struct TermVector(HashMap<Term, RoaringBitmap>);

impl TermVector {
    pub fn new() -> TermVector {
        TermVector(HashMap::new())
    }
}

impl Deref for TermVector {
    type Target = HashMap<Term, RoaringBitmap>;

    fn deref(&self) -> &HashMap<Term, RoaringBitmap> {
        &self.0
    }
}

impl DerefMut for TermVector {
    fn deref_mut(&mut self) -> &mut HashMap<Term, RoaringBitmap> {
        &mut self.0
    }
}

impl Into<TermVector> for Vec<Token> {
    fn into(self) -> TermVector {
        let mut map = HashMap::new();

        for token in self {
            let positions = map.entry(token.term).or_insert_with(RoaringBitmap::new);
            positions.insert(token.position);
        }

         TermVector(map)
    }
}

impl Into<Vec<Token>> for TermVector {
    fn into(self) -> Vec<Token> {
        let mut vec = Vec::new();

        for (term, positions) in self.0 {
            for position in positions {
                vec.push(Token { term: term.clone(), position: position });
            }
        }

        vec.sort_by_key(|token| token.position);

        vec
    }
}
